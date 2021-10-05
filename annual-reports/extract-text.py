# scan GCS for PDF files
# extract the text from those files
# this is an async operation - results will be stored back in GCS

project_id = '33537981247'
location = 'eu'
processor_id = '55ecc5dea8467ea4'
bucket_name = 'ianpattison-annual-reports'
input_prefix = 'pdf/'
output_prefix = 'txt/'

from google.cloud import documentai_v1 as documentai
from google.cloud.documentai_v1.types import document

def setup_and_scan(
):
    # create the DocAI client
    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    docai = documentai.DocumentProcessorServiceClient(client_options=opts)

    # define the documents to scan in
    # input_files = documentai.GcsPrefix(gcs_uri_prefix=input_prefix)
    input_files = documentai.GcsPrefix(gcs_uri_prefix=f"gs://{bucket_name}/{input_prefix}")
    input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=input_files)

    # define the output location
    destination_uri = f"gs://{bucket_name}/{output_prefix}"
    output_config = documentai.DocumentOutputConfig(gcs_output_config={"gcs_uri": destination_uri})

    # define and submit the request
    request = documentai.BatchProcessRequest(
        name=f"projects/{project_id}/locations/{location}/processors/{processor_id}",
        input_documents=input_config,
        document_output_config=output_config
    )
    docai.batch_process_documents(request)

    # # scan the directory for PDF files
    # for file in os.listdir(pdf_dir):
    #     if file.endswith(".pdf"):
    #         # if it's a PDF, process it
    #         process_document(os.path.join(pdf_dir, file), docai)


# def process_document(
#     file_path,
#     docai
# ):
#     # read the file into memory
#     with open(file_path, "rb") as image:
#         image_content = image.read()

#     # define the full name of the DocAI processor
#     name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

#     # configure the process request
#     document = {"content": image_content, "mime_type": "application/pdf"}
#     request = {"name": name, "raw_document": document}

#     # use the client to process the doc
#     result = docai.process_document(request=request)
#     document = result.document

#     # define the text file name
#     base_name = os.path.basename(file_path)
#     org_name = os.path.splitext(base_name)[0]
#     text_path = os.path.join(txt_dir, org_name + ".txt")

#     # store the text
#     with open (text_path, "w") as f:
#         f.write(document.text)


if __name__ == "__main__":
    setup_and_scan()