import json, boto3
from .analytics import *
from botocore.exceptions import ClientError


def new_fn(s3client, bucket, prefix, modifier, suffix):
    """Checks if file already exists, then returns new filename.
    If file already exists, the new filename has count appended to it.

    Args:
        s3client (S3.Client): s3 client to check if file exists
        bucket (str): s3 bucket for new file
        prefix (str): s3 prefix for new file
        modifier (str): extra string for new file (before suffix)
        suffix (str): suffix for new file

    Returns:
        str: new filename based on prefix, modifier and suffix
    """
    try:
        s3client.head_object(Bucket=bucket, Key=prefix + modifier + suffix)
    except ClientError:
        return prefix + modifier + suffix
    modifier += "x_"
    for i in range(1, 10):
        try:
            modifier = modifier[:-2] + str(i) + "_"
            s3client.head_object(Bucket=bucket, Key=prefix + modifier + suffix)
        except ClientError:
            return prefix + modifier + suffix


def write_line(writer, arr):
    """Writes row of csv and returns csv string

    Args:
        writer (str): current string holding data to be written to csv
        arr (list): list of lists with each list one row of data to be written

    Returns:
        str: writer string after appending new data
    """
    return writer + "".join([",".join(str(x) for x in vals) + "\n" for vals in arr])


def lambda_handler(event, context):
    """Handle event-driven lambda function"""
    PROCESSED_PREFIX = "transcribe_comprehend_pipeline/transcriptions_processed/"
    OUTPUT_BUCKET = "cti-comprehend-job-output"
    OUTPUT_PREFIX = "transcribe_comprehend_pipeline/output/"

    # Load boto3 resources and clients
    s3 = boto3.resource("s3")
    s3client = boto3.client("s3")
    comprehend = boto3.client("comprehend")

    # Extract event details
    eventdt = event["Records"][0]["eventTime"]
    fn = event["Records"][0]["s3"]["object"]["key"]
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]

    # Extract json from file
    json_name = (str(fn).split("/"))[-1]
    obj = s3.Object(bucket_name, fn)
    print("Filename:", fn)
    parsed_json = json.loads(obj.get()["Body"].read().decode("utf-8"))
    parsed_results = parsed_json["results"]
    redacted_json = parsed_json.copy()
    transcripts = parsed_results["transcripts"]

    sentiment_dict = {"sentiments": [], "talkover_time": 0}
    comprehend_writer = "Filename,Datetime,Job,Description,Value,Text,Confidence\n"

    for i, t in enumerate(transcripts):
        transcript = t["transcript"]
        print("Original: ", transcript)

        # Perform PII redaction
        response = comprehend.detect_pii_entities(Text=transcript, LanguageCode="en")

        for j in range(len(response["Entities"])):
            if response["Entities"][j]["Type"] is not None:  #'CREDIT_DEBIT_NUMBER':
                transcript = redact(
                    response["Entities"][j]["BeginOffset"],
                    response["Entities"][j]["EndOffset"],
                    transcript,
                )
        print("Redacted: ", transcript)
        redacted_json["results"]["transcripts"][i]["transcript"] = transcript

        # Dominant Language detection job
        dominant_language_out = comprehend.detect_dominant_language(Text=transcript)[
            "Languages"
        ][0]
        dominant_language = dominant_language_out["LanguageCode"]

        # Entity recognition job on redacted transcript
        entities_out = comprehend.detect_entities(
            Text=transcript, LanguageCode=dominant_language
        )
        entities = entities_out["Entities"]

        # Keyphrase extraction on redacted transcript
        keyphrases_out = comprehend.detect_key_phrases(
            Text=transcript, LanguageCode=dominant_language
        )
        keyphrases = keyphrases_out["KeyPhrases"]

        comprehend_writer = write_line(
            comprehend_writer,
            [
                [
                    json_name,
                    eventdt,
                    "language",
                    "",
                    "",
                    dominant_language,
                    dominant_language_out["Score"],
                ]
            ],
        )
        comprehend_writer = write_line(
            comprehend_writer,
            [
                [
                    json_name,
                    eventdt,
                    "entities",
                    entity["Type"],
                    "",
                    entity["Text"],
                    entity["Score"],
                ]
                for entity in entities
            ],
        )
        comprehend_writer = write_line(
            comprehend_writer,
            [
                [
                    json_name,
                    eventdt,
                    "keyphrases",
                    "",
                    "",
                    keyphrase["Text"],
                    keyphrase["Score"],
                ]
                for keyphrase in keyphrases
                if float(keyphrase["Score"]) > 0.9
            ],
        )

    # Chunk sentiment analysis job
    items_list = [parsed_results["items"]]
    for i, items in enumerate(items_list):
        sentiment_dict["sentiments"].append({"top_sentiment": "TODO", "chunks": []})
        chunks, chunk_times = split_items_into_chunks(items)
        for c, chunk in enumerate(chunks):
            sentiment_out = comprehend.detect_sentiment(
                Text=chunk, LanguageCode=dominant_language
            )
            confidences = list(sentiment_out["SentimentScore"].values())
            sentiment_names = list(sentiment_out["SentimentScore"])
            sentiment_dict["sentiments"][i]["chunks"].append(
                {"chunk_end": chunk_times[c], "sentiments": sentiment_out}
            )
            comprehend_writer = write_line(
                comprehend_writer,
                [
                    [
                        json_name,
                        eventdt,
                        "sentiment_chunk",
                        "sentiment",
                        chunk_times[c],
                        sentiment_out["Sentiment"],
                        max(confidences),
                    ]
                ],
            )

        # Calculate overall sentiment
        overall_sentiment_confidences = [
            avg(
                [
                    list(chunk["sentiments"]["SentimentScore"].values())[j]
                    for chunk in sentiment_dict["sentiments"][i]["chunks"]
                ]
            )
            for j in range(4)
        ]
        top_sentiment = sentiment_names[
            overall_sentiment_confidences.index(max(overall_sentiment_confidences))
        ].upper()

        sentiment_dict["sentiments"][i]["top_sentiment"] = top_sentiment
        comprehend_writer = write_line(
            comprehend_writer,
            [
                [
                    json_name,
                    eventdt,
                    "overall_sentiment",
                    "sentiment",
                    "",
                    top_sentiment,
                    max(overall_sentiment_confidences),
                ]
            ],
        )

    # Talkover analysis job
    if "channel_labels" in parsed_results:
        channels_items = [
            channel["items"] for channel in parsed_results["channel_labels"]["channels"]
        ]
        if len(channels_items) == 2:
            talkover_time, talkover_count = calculate_talkover_time(channels_items)
            sentiment_dict["talkover_time"] = talkover_time
            comprehend_writer = write_line(
                comprehend_writer,
                [
                    [
                        json_name,
                        eventdt,
                        "talkover",
                        "total_time",
                        talkover_time,
                        "",
                        "",
                    ],
                    [json_name, eventdt, "talkover", "count", talkover_count, "", ""],
                ],
            )

    # Individual sentiment
    if "channel_labels" in parsed_results:
        channels_items = [
            channel["items"] for channel in parsed_results["channel_labels"]["channels"]
        ]

        for channel_iterator, items in enumerate(channels_items):
            if len(channels_items) <= 1:
                break
            t = words_to_text(items_to_words(items))
            sentiment_out = comprehend.detect_sentiment(Text=t, LanguageCode="en")
            comprehend_writer = write_line(
                comprehend_writer,
                [
                    [
                        json_name,
                        eventdt,
                        "channel_sentiment",
                        "sentiment",
                        channel_iterator,
                        sentiment_out["Sentiment"],
                        max(sentiment_out["SentimentScore"].values()),
                    ]
                ],
            )

    # Move transcription to processed and dump output
    s3.Bucket(OUTPUT_BUCKET).put_object(
        Key=new_fn(
            s3client, OUTPUT_BUCKET, OUTPUT_PREFIX, "comprehend_", json_name + ".csv"
        ),
        Body=comprehend_writer,
    )
    s3.Object(
        bucket_name,
        new_fn(s3client, bucket_name, PROCESSED_PREFIX, "processed_", json_name),
    ).put(Body=json.dumps(redacted_json))
    s3.Object(bucket_name, fn).delete()
