use crate::types::ScalarFilter;
use std::path::Path;
use std::sync::Mutex;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, STORED, STRING, Schema, TEXT};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, Term, doc};

const TEXT_FIELD: &str = "text";
const ID_FIELD: &str = "id";
const FILTERS_FIELD: &str = "filters";

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
    id_field: Field,
    text_field: Field,
    filters_field: Field,
}

impl TextIndex {
    /// Open or create a text index at the given directory.
    pub fn open<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field(ID_FIELD, STRING | STORED);
        let text_field = schema_builder.add_text_field(TEXT_FIELD, TEXT);
        let filters_field = schema_builder.add_text_field(FILTERS_FIELD, STRING);
        let schema = schema_builder.build();

        let dir = MmapDirectory::open(path)?;
        let index = Index::open_or_create(dir, schema.clone())?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let writer = Mutex::new(index.writer(WRITER_HEAP_BYTES)?);

        Ok(Self {
            index,
            reader,
            writer,
            id_field,
            text_field,
            filters_field,
        })
    }

    /// Index a document with optional scalar filters.
    pub fn index_doc(&self, id: &str, text: &str, filters: &[ScalarFilter]) -> crate::Result<()> {
        let mut document = doc!(
            self.id_field => id,
            self.text_field => text,
        );
        for filter in filters {
            document.add_text(self.filters_field, filter_token(filter));
        }

        let mut writer = self.writer.lock().unwrap();
        writer.add_document(document)?;
        writer.commit()?;
        Ok(())
    }

    /// Search the text index, applying scalar filters as exact-match
    /// conditions. Returns a ranked list of `(id, rank)`.
    pub fn search(
        &self,
        query_text: &str,
        filters: &[ScalarFilter],
        k: usize,
    ) -> crate::Result<Vec<(String, usize)>> {
        self.reader.reload()?;
        let searcher: Searcher = self.reader.searcher();

        let text_query = build_text_query(&self.index, self.text_field, query_text)?;
        let final_query = build_filtered_query(text_query, self.filters_field, filters);

        let top_docs = tantivy::collector::TopDocs::with_limit(k);
        let results = searcher.search(&*final_query, &top_docs)?;

        let mut ranked = Vec::with_capacity(results.len());
        for (rank, (_score, doc_address)) in results.iter().enumerate() {
            let doc = searcher.doc(*doc_address)?;
            if let Some(id_value) = doc.get_first(self.id_field)
                && let Some(id) = id_value.as_text()
            {
                ranked.push((id.to_string(), rank + 1));
            }
        }
        Ok(ranked)
    }
}

fn filter_token(filter: &ScalarFilter) -> String {
    match filter {
        ScalarFilter::Eq { field, value } => format!("{field}{FILTER_SEPARATOR}{value}"),
    }
}

fn build_text_query(
    index: &Index,
    text_field: Field,
    query_text: &str,
) -> crate::Result<Box<dyn tantivy::query::Query>> {
    let query_parser =
        QueryParser::new(index.schema(), vec![text_field], index.tokenizers().clone());
    Ok(query_parser.parse_query(query_text)?)
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
