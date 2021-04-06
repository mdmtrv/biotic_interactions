get_or_set = function(key, df){
  general_collection = general_collection
  if (is.null(key) == TRUE) {
    key = uuid::UUIDgenerate()
    #remove dashes and convert to uppercase
    #key = gsub("-", "", key)
    key = toupper(key)
    save_to_mongo(key = identifier(key, prefix)$uri, value = df$label, type = df$type,  orcid = df$orcid, parent = df$parent, publisher_id = df$publisher_id, journal_id = df$journal_id, plazi_doc = df$plazi_doc, doi =  df$doi, article_id =  df$article_id, collection = general_collection)
    id = key
  }else{
    if (!(is.na(df$orcid))){
      key = paste0("<",key,">")
      query = sprintf("{\"%s\":\"%s\"}", "key", key)
      update = sprintf("{\"$set\":{\"%s\":\"%s\"}}", "orcid", df$orcid)
      general_collection$update(query = query, update = update)
    }
    id = rdf4r::strip_angle(key)

    id = gsub("http:\\/\\/openbiodiv\\.net\\/", "", id)
  }
}


get_or_set_mongoid= function (df, prefix)
{
  general_collection = general_collection

  if (nrow(df)>0){
    if (is.null(df) == FALSE){
      #check whether the type is treatment and the id is not na


      if (!(is.na(df$key))){
        key = df$key
        save_to_mongo(key = key, value = df$label, type = df$type, orcid = df$orcid , parent = NA, publisher_id = df$publisher_id, journal_id = df$journal_id, plazi_doc = df$plazi_doc, doi =  df$doi, article_id = df$article_id, collection = general_collection)

        key = toString(key)
        id = rdf4r::strip_angle(key)
        id = gsub("http:\\/\\/openbiodiv\\.net\\/", "", id)
      } else{
        if (!(is.na(df$orcid))) {
          query = sprintf("{\"%s\":\"%s\"}", "orcid", df$orcid)
          key = general_collection$find(query)$key

          #key = check_mongo_key_via_orcid(df$orcid, general_collection)
          if (is.null(key)){
            key = check_mongo_key(value = df$label, type = df$type, collection = general_collection, regex = FALSE)
          }
          id = get_or_set(key, df)
        }
        else {
          if (df$type == "taxonomicConcept"){
            key = check_mongo_key_via_parent(parent = df$parent, type = df$type, collection = general_collection)
            id = get_or_set(key, df)
          } else if (df$type == "tnu"){
            id = get_or_set(NULL, df)
          }
          else
          {

            #nocheck = c("introduction", "abstract", "discussion", "bibliography", "keywords")
            #if (df$type %in% nocheck){
            #  key = NULL
            #  id = get_or_set(key, df)
            #}else{
            key = check_mongo_key(value = df$label, type = df$type, collection = general_collection, regex = FALSE)
            id = get_or_set(key, df)
            # }
          }
        }
      }
    }
  }else{
    id = NULL
  }

  return(id)
}


searchForRowspan = function(rowsMeta, tdIndex, rowIndex){
  if (length(rowsMeta)>0){

    if (length(rowsMeta)>1){
      res =  sapply(rowsMeta, function(r) {
        which(tdIndex %in% r[2] && rowIndex %in% r[1])
      })


    }else{
      #res = which(tdIndex %in% rowsMeta[[1]]$tdIndex && rowIndex %in% rowsMeta[[1]]$rowIndex)
      res = sapply(rowsMeta, function(r) {which(tdIndex %in% rowsMeta[[1]]$tdIndex && rowIndex %in% rowsMeta[[1]]$rowIndex)
      })
    }
    # rowMetaItem = rowsMeta[res]
    rowMetaItem = rowsMeta[unlist(res)]
  } else{
    rowsMetaItem = NULL
  }
}

annotate_table_component= function(content, ontologies) {
  #SEND FOR ANNOTATION
  if (length(content)>0){
    #print(paste("content:", content, " row:", row, " col:", col))
    if (grepl("[a-zA-Z]", content)){
      content = tolower(content)
      res = send_annotation_request(text= content, ontologies = ontologies)
      json_content = jsonlite::fromJSON(res)
      #TODO: if there is a match, break and save table
      if (length(unlist(json_content$data)) > 0 && json_content$meta$statistics$total > 0){
        annotations = json_content$data
      }else{
        annotations = NULL
      }
    }else{
      annotations = NULL
    }
  }else {
    annotations = NULL
  }
  return(annotations)
}




process_table_simplified = function(table, ontologies, table_id, table_content, caption, table_number, article_doi, collection){
  #TODO write fn
  headers = xml2::xml_text(xml2::xml_find_all(table, "//tr/th"))
  data = xml2::xml_text(xml2::xml_find_all(table, "//tr/td"))
  toAnnotate = c(headers, data)
  annotations = list()
  annotation = annotate_table_component(caption, ontologies = ontologies)
  if (!(is.null(annotation))){
    annotations = list.append(annotations, c(annotation))
  }

  for (content in toAnnotate){
    annotation = annotate_table_component(content, ontologies = ontologies)
    if (!(is.null(annotation))){
      names(annotation) = c("id", "lbl", "length", "possition", "ontology", "type", "context", "is_synonym", "is_word")
      annotations = list.append(annotations, c(annotation))
    }
  }
  if (length(annotations) > 0){

    d = data.frame(
      table_id = table_id,
      table_content = table_content,
      caption =  caption,
      table_number = table_number,
      article_doi = article_doi,
      processing_date = Sys.Date()
    )

    d$annotations = list(annotations)
    # d$annotations = json_content

    collection$insert(d)
  }
  #return(annotations)
}

process_table = function(table, ontologies, table_id, table_content, caption, table_number, article_doi, collection){

  rows = xml2::xml_find_all(table, ".//tr")

  rowsMeta = list()
  counter = 1
  add_one = FALSE
  annotations = list()
  annotation = annotate_table_component(caption, ontologies = ontologies)
  if (!(is.null(annotation))){
    names(annotation) = c("id", "lbl", "length", "possition", "ontology", "type", "context", "is_synonym", "is_word")

    annotations = list.append(annotations, c(annotation))
  }

  for(rowNum in 1:length(rows)){
    tds = xml2::xml_find_all(rows[rowNum], ".//td | .//th")
    for (tdIndex in 1:length(tds)){
      td = tds[tdIndex]
      content = xml2::xml_text(td)

      if (xml2::xml_has_attr(td, "colspan")==TRUE){
        if (as.numeric(xml2::xml_attr(td, 'colspan'))>1){
          colspan = as.numeric(unlist(xml2::xml_attr(td, "colspan")))
          max_col = (tdIndex+colspan)-1
          tdIndex = rep(tdIndex:((tdIndex+colspan)-1))
        }
      }

      rowspan = 1
      if (xml2::xml_has_attr(td, "rowspan")==TRUE){
        if (as.numeric(xml2::xml_attr(td, 'rowspan'))>1){
          rowspan = as.numeric(unlist(xml2::xml_attr(td, "rowspan")))
          if(is.null(rowspan)){
            rowspan = 1
          }
          rowsMeta = list.append(rowsMeta, list(rowIndex = rep(rowNum:(rowNum+rowspan-1)), tdIndex = tdIndex, rowspan = rowspan))


          #  counter = counter+1
        }
      }

      rowMetaItem = searchForRowspan(rowsMeta, tdIndex, rowNum)

      if (!(is.null(unlist(rowMetaItem)))){
        tempRow = rowMetaItem[[1]]$rowIndex
        if (rowNum == min(tempRow) || rowNum == max(tempRow)){
          rowMetaItem[[1]]$rowspan= rowMetaItem[[1]]$rowspan-1
          if (rowMetaItem[[1]]$rowspan < 2){
            rowMetaItem = NULL
          }
          rowNumber = tempRow
        } else{
          rowNumber = rowNum
        }
      }
      else{
        rowNumber = rowNum
      }


      if (add_one == TRUE){
        tdIndex = tdIndex + 1
      }
      count = 0
      if (length(rowNumber)>1){
        if (!(max(rowNumber)==rowNum)){
          annotation = annotate_table_component(content, ontologies)
          if (!(is.null(annotation))){
            names(annotation) = c("id", "lbl", "length", "possition", "ontology", "type", "context", "is_synonym", "is_word")

            annotations = list.append(annotations, c(annotation, row = toString(rowNumber), col = toString(tdIndex)))
          }
        }else{
          annotation = annotate_table_component(content, ontologies)
          if (!(is.null(annotation))){
            names(annotation) = c("id", "lbl", "length", "possition", "ontology", "type", "context", "is_synonym", "is_word")
            annotations = list.append(annotations, c(annotation, row = toString(max(rowNumber)), col = toString(tdIndex+1)))
          }
          add_one = TRUE
        }
      }else {
        annotation = annotate_table_component(content, ontologies)
        if (!(is.null(annotation))){
          names(annotation) = c("id", "lbl", "length", "possition", "ontology", "type", "context", "is_synonym", "is_word")

          annotations = list.append(annotations, c(annotation, row = toString(rowNumber), col = toString(tdIndex)))
        }
      }
    }
  }

  if (length(annotations) > 0){

    d = data.frame(
      table_id = table_id,
      table_content = table_content,
      caption =  caption,
      table_number = table_number,
      article_doi = article_doi,
      processing_date = Sys.Date()
    )

    d$annotations = list(annotations)
    bi_collection$insert(d)
  }
}


api_annotator_access = function(url, key)
{
  access = c()
  access$key = c(Authorization = paste("Bearer", key, sep = " "))
  access$url = url
  return(access)
}

send_annotation_request = function(access = api_annotator_access(url, key), text, ontologies){
  res = httr::POST(access$url, httr::add_headers(access$key), body = list(text=text, ontologies=ontologies))
  return(httr::content(res, "text"))
}
