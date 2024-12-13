        SELECT 
        file_name, 
        -- Ensure the AI query response is returned correctly as a valid JSON array
        ai_query('poc_metadata_batch_inference', 
            CONCAT('You are given a piece of text from a document. Please extract and return the requested information in JSON format following the exact schema below. Do not include any explanatory text, introductions, backticks, or code fences. Only output the JSON object itself.
                    If a piece of information is not available, leave it as an empty string or empty arrays/objects as appropriate.

                    Sections and requirements:

                    1. Document Management (always an array with one object):
                    - Document Number
                    - Author Name
                    - Author Role
                    - Author Department
                    - Reviewer Name
                    - Reviewer Role
                    - Reviewer Department
                    - Approver Name
                    - Approver Role
                    - Approver Department
                    - Issue Date (If not found, use the revision date of the final revision if available)
                    - Revision
                    - Asset ID
                    - Final Report (Yes/No)

                    2. Document Change History (array of objects, or empty array if none found):
                    - Revision
                    - Date
                    - Author
                    - Section
                    - Description

                    3. Summary (object, or empty object if not found):
                    - Station
                    - Asset
                    - Asset ID
                    - Actions (array of objects, or empty array if none found):
                        - Reference (ALA code, e.g. A001, AC001)
                        - Maximo Ref
                        - Description
                        - Category (1, 2, or 3)
                        - Completion Due Date
                        - Owner
                    - Recommendations (array of objects, or empty array if none found):
                        - Recommendation ID (e.g. R001, REC001)
                        - Rec Title (If not explicitly given, attempt to infer from bolded text or name-like text following the Recommendation ID. If none found, leave empty.)
                        - Rec Description
                    - Risk Ranking (object):
                        - Risk Rank (RAG):
                        * "Good" = Green
                        * "Caution" = Amber
                        * "Bad" = Red
                        * If not explicitly stated, infer from context. For severe issues, choose Red; minor concerns, Amber; no issues, Green.
                        * If no inference possible, leave empty.
                        - Certainty Score (0 to 100):
                        * 100 if an exact keyword match (e.g., word "Good" found)
                        * Lower if inferred from context (e.g., 90 for strong contextual clues, 50 for weak clues)
                        * If no inference, leave Risk Rank empty and Certainty Score = 0
                        - Reason: A brief explanation of why this Risk Rank was chosen.
                        - Source: The sentence or phrase from the text that led to this determination. If multiple sources, choose the most relevant.

                    Text:
                    "

                    Return only the JSON in this exact structure (no extra text):

                    {
                    "Document Management": [
                        {
                        "Document Number": "",
                        "Author Name": "",
                        "Author Role": "",
                        "Author Department": "",
                        "Reviewer Name": "",
                        "Reviewer Role": "",
                        "Reviewer Department": "",
                        "Approver Name": "",
                        "Approver Role": "",
                        "Approver Department": "",
                        "Issue Date": "",
                        "Revision": "",
                        "Asset ID": "",
                        "Final Report": ""
                        }
                    ],
                    "Document Change History": [
                        {
                        "Revision": "",
                        "Date": "",
                        "Author": "",
                        "Section": "",
                        "Description": ""
                        }
                    ],
                    "Summary": {
                        "Station": "",
                        "Asset": "",
                        "Asset ID": "",
                        "Actions": [
                        {
                            "Reference": "",
                            "Maximo Ref": "",
                            "Description": "",
                            "Category": "",
                            "Completion Due Date": "",
                            "Owner": ""
                        }
                        ],
                        "Recommendations": [
                        {
                            "Recommendation ID": "",
                            "Rec Title": "",
                            "Rec Description": ""
                        }
                        ],
                        "Risk Ranking": {
                        "Risk Rank (RAG)": "",
                        "Certainty Score": 0,
                        "Reason": "",
                        "Source": ""
                        }
                    }
                    }

                    ',
                'Chunk Text: ', chunk_1),
                modelParameters => named_struct('temperature', 0.1)
        ) AS metadata 
    FROM kyra_wulffert.poc_ala.bronze_docs