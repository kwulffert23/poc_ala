    SELECT 
        file_name, 
        -- Ensure the AI query response is returned correctly as a valid JSON array
        ai_query('poc_metadata_batch_inference', 
            CONCAT('Below is an additional portion of the document text. Previously, the Summary section was not found. Now, using this new chunk of text, please attempt to extract the Summary information as defined below. Return only a JSON object as defined. Do not include any explanatory text, introductions, or code fences. If information is missing, leave the value as an empty string or empty arrays/objects.

                    Summary fields to extract (if available in this chunk):
                    - Station
                    - Asset
                    - Asset ID
                    - Actions (array of objects, if available, otherwise empty array):
                    Each Action object:
                    - Reference (ALA code, e.g. A001, AC001)
                    - Maximo Ref
                    - Description
                    - Category (1, 2, or 3)
                    - Completion Due Date
                    - Owner

                    - Recommendations (array of objects, if available, otherwise empty array):
                    Each Recommendation object:
                    - Recommendation ID (e.g. R001, REC001)
                    - Rec Title (If not explicitly given, attempt to infer from bolded text or something name-like following the Recommendation ID. If no title or bolded text is present, leave empty.)
                    - Rec Description

                    - Risk Ranking (object):
                    - Risk Rank (RAG): 
                        * If words like "Good" appear, interpret as Green.
                        * If words like "Caution" appear, interpret as Amber.
                        * If words like "Bad" appear, interpret as Red.
                        * If no explicit words are given, infer from context (e.g., mention of severe issues = Red/Bad, minor concerns = Amber/Caution, no issues = Green/Good).
                    - Certainty Score (0 to 100):
                        * 100 if an exact keyword match (e.g., the word "Good" explicitly appears).
                        * Lower if inferred from context. Set a value that represents confidence (e.g., 90 for very strong contextual clues, 50 for weak clues).
                        * If no inference possible, leave Risk Rank empty and Certainty Score = 0.
                    - Reason:
                        * Describe why you chose this Risk Rank. For example, "Exact keyword `Good` found," or "Inferred from mention of multiple major defects," etc.
                    - Source:
                        * Include the sentence or phrase from the provided text that led to this decision. If multiple sentences contribute, pick the one most influential in your inference.

                    If something cannot be determined, leave it empty.

                    Text:
                   

                    Return only the JSON in the following format:

                    {
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
                'Chunk Text: ', chunk_2),
                modelParameters => named_struct('temperature', 0.1)
        ) AS inferred_summary 
        FROM kyra_wulffert.poc_ala.bronze_docs