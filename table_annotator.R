setwd("/home/mid/Documents/work/biotic_interactions")
source("bi_functions.R")
library(rdf4r)
library(ropenbio)
library(rlist)
configuration = yaml::yaml.load_file("../deployment.yml")
api_authorization = yaml::yaml.load_file("../api_authorization.yaml")
mongo_authorization = yaml::yaml.load_file("../mongo_authorization.yaml")


url = api_authorization$url
key =  api_authorization$key

obkms = basic_triplestore_access(
server_url = configuration$server_url,
repository = configuration$repository,
user = configuration$user,
password = configuration$password
)
access_options = obkms

prefix = c(openbiodiv="http://openbiodiv.net/")
ontologies = c("custom")
table_collection = mongolite::mongo(collection = mongo_authorization$table_collection, url = mongo_authorization$url)
bi_collection = mongolite::mongo(collection = mongo_authorization$bi_collection, url = mongo_authorization$url)
general_collection = mongolite::mongo(collection = mongo_authorization$general_collection, url = mongo_authorization$url)
xml_collection =  mongolite::mongo(collection = mongo_authorization$xml_collection, url = mongo_authorization$url)

#we get the xmls from mongodb because the annotator is not available from the remote server but mongo is
filenames = table_collection$find()
filenames = unlist(filenames)
for (f in filenames){
  query = sprintf("{\"%s\":\"%s\"}", "filename", f)
  xml = xml_collection$find(query)$xml
  xml =  xml2::as_xml_document(xml)
  triples = ResourceDescriptionFramework$new()

  processing_xml = xml
  pensoft_xpath = "//article/front/article-meta/uri[@content-type='arpha']"
  article_id = xml2::xml_text(xml2::xml_find_first(processing_xml, pensoft_xpath))
  article_doi = xml2::xml_text(xml2::xml_find_all(processing_xml, "//article/front/article-meta/article-id[@pub-id-type='doi']"))
  # REMOVE THIS IF
  if (length(article_doi) < 1)
  {
    print("skip")
  }else{

  article_ident = xml2::xml_text(xml2::xml_find_all(xml, "//article/front/article-meta/article-id[@pub-id-type='publisher-id']"))
  id = identifier(article_id, prefix = prefix)
  triples$set_context(id)
  counter = 0
  tables = xml2::xml_find_all(processing_xml, "//table")
  for (table in tables){
    counter = counter+1
    table_content = toString(table)
    caption = xml2::xml_text(xml2::xml_find_all(table, "./parent::table-wrap/caption"))
    if (length(caption)==0){
      label = table_content
      caption = NA
    }else{
      label = caption
    }
    df = ropenbio::set_component_frame(key = NA, label = label, mongo_key = c(table = NA), type = "table", orcid = NA, parent = article_id, publisher_id = NA, journal_id = NA, plazi_doc = FALSE, doi = article_doi, article_id =article_ident)
    table_id= get_or_set_mongoid(df, prefix )
    table_id= identifier(table_id, prefix)
    #write triple that the table is contained by the article
    triples$add_triple(table_id, is_contained_by, id)
    triples$add_triple(table_id, rdf_type, Table)
    serialization = triples$serialize()
    add_data(serialization, access_options = access_options)
    table_number = xml2::xml_text(xml2::xml_find_all(table, "./@id | ./parent::table-wrap/@id"))
    if (length(table_number)==0){
      table_number = counter
    }
    if (length(xml2::xml_find_all(table, "//tr/th[@colspan>'1' and @rowspan>'1'] | //tr/td[@colspan>'1' and @rowspan>'1']")) > 0){
      process_table_simplified(table, ontologies, table_id, table_content, caption, table_number, article_doi, bi_collection)
    }else{
      process_table(table, ontologies, table_id, table_content, caption, table_number, article_doi, bi_collection)
    }
    }
  }
}

