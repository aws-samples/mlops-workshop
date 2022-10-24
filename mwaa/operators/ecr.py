import boto3


def get_model_digest(model_name):
    ecr = boto3.client('ecr')
    response = ecr.list_images(
        repositoryName=model_name,
        filter={
            'tagStatus': "TAGGED"
        }
    )
    images = response["imageIds"]
    latest_images = [image for image in images if image.get("imageTag") == "latest"]

    if len(latest_images) == 1:
        image_id = latest_images[0]["imageDigest"].strip("sha256:")[0:16]
    else:
        image_id = None

    print(image_id)
    return image_id