
import s3fs
s3 = s3fs.S3FileSystem(anon=True)

s3.ls('s3://datasci-assignment/click_log/2024/05/10/00')