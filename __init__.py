from custom_nodes.s3up.nodes.s3_upload_node import S3UploadNode

NODE_CLASS_MAPPINGS = {
    "S3UploadNode": S3UploadNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "S3UploadNode": "S3存储",
}

