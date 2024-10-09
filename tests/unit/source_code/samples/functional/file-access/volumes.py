df = (
   spark.readStream.format('cloudFiles')
   .option('cloudFiles.format', 'csv')
   .option('cloudFiles.schemaLocation', '/Volumes/playground/test/schemas/')
   .option('header', 'true')
   .option('compression', 'gzip')
   .load('/Volumes/playground/test/demo_data/')
)
