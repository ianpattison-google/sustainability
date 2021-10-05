# scan GCS for TXT files previously extracted
# extract the report paragraphs from those files
# classify the paragraphs and stoer results in BigQuery

project_name = 'ianpattison-sandbox'
bucket_name = 'ianpattison-annual-reports'
input_prefix = 'txt/'
table_id = 'ianpattison-sandbox.annual_reports.paragraphs'

from google.cloud import storage
from google.cloud import language
from google.cloud import bigquery
from google.cloud import documentai_v1 as documentai
from google.cloud.documentai_v1.types import document

import re


def setup_and_scan(
):
    # create the GCS client
    gcs = storage.Client()

    # create a NLP client to classify the text
    nlp = language.LanguageServiceClient()

    # create a BigQuery client to store the result
    bq = bigquery.Client(project_name)

    # define the documents to scan in
    blobs = gcs.list_blobs(bucket_name, prefix=input_prefix)
    
    # set up an array to hold the results
    rows_to_insert = []
    
    # iterate over the documents
    for blob in blobs:
        b = blob.download_as_bytes()
        document = documentai.Document.from_json(b)

        # extract the name of the organisation from the file name
        file_name = blob.name.split('/')[-1]  # extract the file name after the final slash, e.g. 'Alphabet-0.json'
        m = re.search('(.+?)-\d+\.json', file_name)  # strip away the ending from the hyphen onwards
        org_name = m.group(1)

        # iterate over the pages and paragraphs
        for page in document.pages:
            paragraphs = page.paragraphs
            for paragraph in paragraphs:
                paragraph_text = get_text(paragraph.layout, document)

                # we can only classify paragraphs of 20 words or more
                if (word_count(paragraph_text) >= 20):
                    classify_text(nlp, paragraph_text, org_name, rows_to_insert)

        print(f"Processed {file_name}")

    err = bq.insert_rows_json(table_id, rows_to_insert)
    print("Finished")


def get_text(doc_element: dict, document: dict):
    response = ""
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response

    
def classify_text(nlp, text, org_name, rows_to_insert):
    document = language.Document(content=text, type_=language.Document.Type.PLAIN_TEXT)
    response = nlp.classify_text(document=document)

    if len(response.categories) == 0:
        rows_to_insert.append({"org": org_name, "text": text, "category": "Not classified", "confidence": 0})

    for category in response.categories:
        rows_to_insert.append({"org": org_name, "text": text, "category": category.name, "confidence": category.confidence})


def word_count(text):
    words = text.split()
    return len(words)


if __name__ == "__main__":
    setup_and_scan()