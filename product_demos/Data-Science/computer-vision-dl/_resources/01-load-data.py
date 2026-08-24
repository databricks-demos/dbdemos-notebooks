# Databricks notebook source
dbutils.widgets.text("volume_folder", "")
volume_folder = dbutils.widgets.get("volume_folder")

# COMMAND ----------

# MAGIC %pip install boto3

# COMMAND ----------

# MAGIC %md
# MAGIC # Download & extract the VisA dataset (serverless-safe, streaming)
# MAGIC `%sh` and the aws-cli aren't available on serverless, and staging the full 1.9GB tar on
# MAGIC the driver's local disk (`/local_disk0`) then `dbutils.fs.cp` to the Volume is unreliable
# MAGIC there. Instead we stream the public (unsigned) S3 object through Python's `tarfile` in
# MAGIC streaming mode (`r|*`) and write only the `pcb1` members we need **straight into the UC
# MAGIC Volume** (`/Volumes/...`, a real POSIX mount) with plain file IO — no large local copy,
# MAGIC no `dbutils.fs.cp`.

# COMMAND ----------

import os, tarfile, shutil, boto3
from botocore import UNSIGNED
from botocore.config import Config

images_dir = f"{volume_folder}/images"
labels_dir = f"{volume_folder}/labels"

# Clean any previous run and (re)create the target dirs directly on the Volume.
for d in (images_dir, labels_dir):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)

# The demo only needs the pcb1 subset:
#   tar: pcb1/Data/Images/<Normal|Anomaly>/*.JPG  ->  {volume_folder}/images/<Normal|Anomaly>/*.JPG
#   tar: pcb1/image_anno.csv                      ->  {volume_folder}/labels/image_anno.csv
IMAGES_PREFIX = "pcb1/Data/Images/"
LABELS_MEMBER = "pcb1/image_anno.csv"

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
body = s3.get_object(Bucket="amazon-visual-anomaly", Key="VisA_20220922.tar")["Body"]

n_images, got_labels = 0, False
# Streaming tar: sequential single pass, never materializes the full 1.9GB locally.
with tarfile.open(fileobj=body, mode="r|*") as tar:
    for member in tar:
        if not member.isfile():
            continue
        if member.name.startswith(IMAGES_PREFIX):
            rel = member.name[len(IMAGES_PREFIX):]          # e.g. "Normal/0000.JPG"
            dest = os.path.join(images_dir, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                shutil.copyfileobj(tar.extractfile(member), f)
            n_images += 1
        elif member.name == LABELS_MEMBER:
            with open(os.path.join(labels_dir, "image_anno.csv"), "wb") as f:
                shutil.copyfileobj(tar.extractfile(member), f)
            got_labels = True

print(f"Extracted {n_images} images to {images_dir}; labels written: {got_labels}")
assert n_images > 0 and got_labels, "Expected pcb1 images and image_anno.csv in the VisA tar"
