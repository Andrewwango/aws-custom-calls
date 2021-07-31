import boto3, uuid


def lambda_handler(event, context):
    """Handle event-driven lambda function"""
    PROCESSED_PREFIX = "transcribe_comprehend_pipeline/transcriptions_processed/"

    # Load boto3 resources and clients
    s3 = boto3.resource("s3")
    client = boto3.client("transcribe")

    # Extract event details
    s3bucket = event["Records"][0]["s3"]["bucket"]["name"]
    fn = event["Records"][0]["s3"]["object"]["key"]

    # Extract filename for processed
    json_name = (str(fn).split("/"))[-1]

    # Start asynch. transcription job
    response = client.start_transcription_job(
        TranscriptionJobName=str(uuid.uuid4()),
        LanguageCode="en-US",
        MediaFormat="wav",
        Media={"MediaFileUri": "s3://" + s3bucket + "/" + fn},
        ContentRedaction={"RedactionType": "PII", "RedactionOutput": "redacted"},
        Settings={
            "VocabularyName": "gaming_vocab",
            "VocabularyFilterName": "filter_1",
            "ChannelIdentification": True,
            "VocabularyFilterMethod": "mask",
        },
        OutputBucketName="cti-comprehend-job-input",
        OutputKey="transcribe_comprehend_pipeline/transcriptions_raw/",
    )

    # Move file to processed folder
    s3.Object(s3bucket, PROCESSED_PREFIX + json_name).copy_from(
        CopySource={"Bucket": s3bucket, "Key": fn}
    )

    ## TODO ##
    # - delete audio file from raw after transcriptions
    # (problematic because transcription is asynchronous and time consuming)
    """ 
    time.sleep(240)
    s3.Object('aj-transcribe-upload', 'transcribe_comprehend_pipeline/transcriptions_processed/'+json_name).copy_from(CopySource={'Bucket':'aj-transcribe-upload', 'Key': fn})
    s3.Object('aj-transcribe-upload', fn).delete()
    

    while True:
        status = client.get_transcription_job(TranscriptionJobName=jobName)
        if status["TranscriptionJob"]["TranscriptionJobStatus"] in ["FAILED"]:
            break
        print("It's in progress")
    while True:
        status = client.get_transcription_job(TranscriptionJobName=jobName)
        if status["TranscriptionJob"]["TranscriptionJobStatus"] in ["COMPLETED"]:
            s3.Object('aj-transcribe-upload', fn).delete()
        

        time.sleep(5)
            
            
            
    """
    """
    return {
        'TranscriptionJobName': response['TranscriptionJob']['TranscriptionJobName']
    }
    """
