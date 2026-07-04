use crate::types::{FieldBoosts, IndexDoc, ScalarFilter};
use std::path::Path;
use std::sync::Mutex;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer,
};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TantivyError, Term};

const ID_FIELD: &str = "id";
const NAME_FIELD: &str = "name";
const BODY_FIELD: &str = "body";
const TAGS_FIELD: &str = "tags";
const FILTERS_FIELD: &str = "filters";

/// Name of the Spanish-aware analyzer registered on the index: lowercase,
/// ASCII-fold accents, then Snowball Spanish stemming. Folding runs before
/// stemming so accented and accent-less spellings normalize identically.
const ES_FOLDED: &str = "es_folded";

const WRITER_HEAP_BYTES: usize = 15_000_000;

/// Separator between field name and value inside a filter token. U+001F (unit
/// separator) cannot appear in normal field names or values, so tokens never
/// collide across fields.
const FILTER_SEPARATOR: char = '\u{1f}';

/// Wrapper around a `tantivy` index for full-text search with optional scalar
/// filters on arbitrary fields.
pub struct TextIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter>,
    analyzer: TextAnalyzer,
    id_field: Field,
    name_field: Field,
    body_field: Field,
    tags_field: Field,
    filters_field: Field,
}

fn build_schema() -> Schema {
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(ES_FOLDED)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );

    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field(ID_FIELD, STRING | STORED);
    schema_builder.add_text_field(NAME_FIELD, text_options.clone());
    schema_builder.add_text_field(BODY_FIELD, text_options.clone());
    schema_builder.add_text_field(TAGS_FIELD, text_options);
    schema_builder.add_text_field(FILTERS_FIELD, STRING | STORED);
    schema_builder.build()
}

fn build_analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .filter(Stemmer::new(Language::Spanish))
        .build()
}

/// Open the index at `path`, wiping and recreating it if an index with an
/// incompatible (pre-multi-field) schema is found. The engine is the only
/// writer of these directories, so a schema mismatch always means an old
/// format.
fn open_or_recreate(path: &Path, schema: &Schema) -> crate::Result<Index> {
    let dir = MmapDirectory::open(path)?;
    match Index::open_or_create(dir, schema.clone()) {
        Ok(index) => Ok(index),
        Err(TantivyError::SchemaError(_)) => {
            std::fs::remove_dir_all(path)?;
            std::fs::create_dir_all(path)?;
            let dir = MmapDirectory::open(path)?;
            Ok(Index::open_or_create(dir, schema.clone())?)
        }
        Err(e) => Err(e.into()),
    }
}

impl TextIndex {
    /// Open or create a text index at the given directory.
    pub fn open<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let schema = build_schema();
        let index = open_or_recreate(path.as_ref(), &schema)?;
        Self::from_index(index)
    }

    /// Create a text index held entirely in memory (no files on disk).
    pub fn open_in_ram() -> crate::Result<Self> {
        let index = Index::create_in_ram(build_schema());
        Self::from_index(index)
    }

    fn from_index(index: Index) -> crate::Result<Self> {
        let analyzer = build_analyzer();
        index.tokenizers().register(ES_FOLDED, analyzer.clone());

        let schema = index.schema();
        let id_field = schema.get_field(ID_FIELD)?;
        let name_field = schema.get_field(NAME_FIELD)?;
        let body_field = schema.get_field(BODY_FIELD)?;
        let tags_field = schema.get_field(TAGS_FIELD)?;
        let filters_field = schema.get_field(FILTERS_FIELD)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let writer = Mutex::new(index.writer(WRITER_HEAP_BYTES)?);

        Ok(Self {
            index,
            reader,
            writer,
            analyzer,
            id_field,
            name_field,
            body_field,
            tags_field,
            filters_field,
        })
    }

    /// Insert or replace a document. A document with no text fields is still
    /// registered (id + filters) so it can be filtered and deleted later.
    pub fn upsert(&self, doc: &IndexDoc) -> crate::Result<()> {
        let writer = self.writer.lock().unwrap();
        self.upsert_with_writer(&writer, doc)?;
        drop_and_commit(writer)
    }

    /// Insert or replace a batch of documents under a single commit.
    pub fn upsert_batch(&self, docs: &[IndexDoc]) -> crate::Result<()> {
        let writer = self.writer.lock().unwrap();
        for doc in docs {
            self.upsert_with_writer(&writer, doc)?;
        }
        drop_and_commit(writer)
    }

    fn upsert_with_writer(&self, writer: &IndexWriter, doc: &IndexDoc) -> crate::Result<()> {
        writer.delete_term(Term::from_field_text(self.id_field, &doc.id));

        let mut document = tantivy::schema::Document::default();
        document.add_text(self.id_field, &doc.id);
        if let Some(name) = &doc.name {
            document.add_text(self.name_field, name);
        }
        if let Some(body) = &doc.body {
            document.add_text(self.body_field, body);
        }
        if let Some(tags) = &doc.tags {
            document.add_text(self.tags_field, tags);
        }
        for filter in &doc.filters {
            document.add_text(self.filters_field, filter_token(filter));
        }
        writer.add_document(document)?;
        Ok(())
    }

    /// Delete a document by id. Deleting a missing id is a no-op.
    pub fn delete_doc(&self, id: &str) -> crate::Result<()> {
        let writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_text(self.id_field, id));
        drop_and_commit(writer)
    }

    /// Delete every document carrying the given scalar filter.
    pub fn delete_by_filter(&self, filter: &ScalarFilter) -> crate::Result<()> {
        let writer = self.writer.lock().unwrap();
        writer.delete_term(Term::from_field_text(
            self.filters_field,
            &filter_token(filter),
        ));
        drop_and_commit(writer)
    }

    /// Remove every document from the index.
    pub fn clear(&self) -> crate::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.delete_all_documents()?;
        writer.commit()?;
        Ok(())
    }

    /// Search the text index, applying scalar filters as exact-match
    /// conditions. Returns a ranked list of `(id, rank, bm25_score)`.
    ///
    /// The query text is parsed leniently: malformed syntax (unbalanced
    /// quotes, stray operators) never fails — it degrades to a bag-of-words
    /// OR query over the analyzed tokens.
    pub fn search(
        &self,
        query_text: &str,
        filters: &[ScalarFilter],
        boosts: FieldBoosts,
        k: usize,
    ) -> crate::Result<Vec<(String, usize, f32)>> {
        self.reader.reload()?;
        let searcher: Searcher = self.reader.searcher();

        let text_query = self.build_text_query(query_text, boosts);
        let final_query = build_filtered_query(text_query, self.filters_field, filters);

        let top_docs = tantivy::collector::TopDocs::with_limit(k.max(1));
        let results = searcher.search(&*final_query, &top_docs)?;

        let mut ranked = Vec::with_capacity(results.len());
        for (rank, (score, doc_address)) in results.iter().enumerate() {
            let doc = searcher.doc(*doc_address)?;
            if let Some(id_value) = doc.get_first(self.id_field)
                && let Some(id) = id_value.as_text()
            {
                ranked.push((id.to_string(), rank + 1, *score));
            }
        }
        Ok(ranked)
    }

    /// Return the ids of every document carrying the given scalar filter.
    pub fn ids_by_filter(&self, filter: &ScalarFilter) -> crate::Result<Vec<String>> {
        self.reader.reload()?;
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(self.filters_field, &filter_token(filter)),
            IndexRecordOption::Basic,
        );
        let addresses = searcher.search(&query, &tantivy::collector::DocSetCollector)?;

        let mut ids = Vec::with_capacity(addresses.len());
        for addr in addresses {
            let doc = searcher.doc(addr)?;
            if let Some(id_value) = doc.get_first(self.id_field)
                && let Some(id) = id_value.as_text()
            {
                ids.push(id.to_string());
            }
        }
        Ok(ids)
    }

    /// Return the filter tokens stored for a document, or `None` if the id is
    /// not indexed. Used to post-filter vector results.
    pub fn stored_filter_tokens(&self, id: &str) -> crate::Result<Option<Vec<String>>> {
        self.reader.reload()?;
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(self.id_field, id),
            IndexRecordOption::Basic,
        );
        let results = searcher.search(&query, &tantivy::collector::TopDocs::with_limit(1))?;
        match results.first() {
            Some((_score, addr)) => {
                let doc = searcher.doc(*addr)?;
                let tokens = doc
                    .get_all(self.filters_field)
                    .filter_map(|v| v.as_text().map(str::to_string))
                    .collect();
                Ok(Some(tokens))
            }
            None => Ok(None),
        }
    }

    fn build_text_query(
        &self,
        query_text: &str,
        boosts: FieldBoosts,
    ) -> Box<dyn tantivy::query::Query> {
        let mut query_parser = QueryParser::new(
            self.index.schema(),
            vec![self.name_field, self.body_field, self.tags_field],
            self.index.tokenizers().clone(),
        );
        query_parser.set_field_boost(self.name_field, boosts.name);
        query_parser.set_field_boost(self.body_field, boosts.body);
        query_parser.set_field_boost(self.tags_field, boosts.tags);

        let (query, errors) = query_parser.parse_query_lenient(query_text);
        if errors.is_empty() {
            return query;
        }
        // Malformed syntax: degrade to a bag-of-words OR query built from the
        // analyzed tokens, so raw user input never fails.
        self.bag_of_words_query(query_text, boosts)
    }

    fn bag_of_words_query(
        &self,
        query_text: &str,
        boosts: FieldBoosts,
    ) -> Box<dyn tantivy::query::Query> {
        let mut analyzer = self.analyzer.clone();
        let mut tokens: Vec<String> = Vec::new();
        let mut stream = analyzer.token_stream(query_text);
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }

        let fields = [
            (self.name_field, boosts.name),
            (self.body_field, boosts.body),
            (self.tags_field, boosts.tags),
        ];
        let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
        for token in &tokens {
            for (field, boost) in fields {
                let term = Term::from_field_text(field, token);
                let term_query = TermQuery::new(term, IndexRecordOption::WithFreqs);
                clauses.push((
                    Occur::Should,
                    Box::new(BoostQuery::new(Box::new(term_query), boost)),
                ));
            }
        }
        Box::new(BooleanQuery::new(clauses))
    }
}

/// Commit while still holding the writer lock.
fn drop_and_commit(mut writer: std::sync::MutexGuard<'_, IndexWriter>) -> crate::Result<()> {
    writer.commit()?;
    Ok(())
}

pub(crate) fn filter_token(filter: &ScalarFilter) -> String {
    match filter {
        ScalarFilter::Eq { field, value } => format!("{field}{FILTER_SEPARATOR}{value}"),
    }
}

fn build_filtered_query(
    text_query: Box<dyn tantivy::query::Query>,
    filters_field: Field,
    filters: &[ScalarFilter],
) -> Box<dyn tantivy::query::Query> {
    if filters.is_empty() {
        return text_query;
    }

    let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = vec![(Occur::Must, text_query)];
    for filter in filters {
        let term = Term::from_field_text(filters_field, &filter_token(filter));
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }
    Box::new(BooleanQuery::new(clauses))
}
