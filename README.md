Tools for understanding the focus of organisations' annual reports

## extract-text.py

- Scans a defined GCS bucket for items in the 'pdf' folder
- Batches these up and submits an offline processing request to Document AI, usingthe OCR processor to return the document text
- Output is wriiten in Python's object format to the same GCS bucket, in the 'txt' folder
  - NB the OCR processor splits the input documents into 10 page chunks, each one will have its own output file

## process-text.py

- Retrieves the output content from the previous step
  - NB this is to allow repeat porcessing of the text without needing to extract from pdf every time
- Iterates through each page and each paragraph
- If the text is longer than 20 words, passes the text to a Natural Language text classifier for analysis
- Stores the results in a BigQuery table for further analysis