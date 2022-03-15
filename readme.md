# Climate data analysis

Data pipeline for giving city level mean and median observations for a specific day

### Approach
Landing zone:
- Source file and put it in landing zone in parquet format (snappy compression)

Raw Zone
- Write to Raw zone with appropriate datatypes 
- Impute missing values(`replaced missing mean temprature with avg of previous and next day`)

Curated Zone:
- Calculate distance between observatory and each city (haversine-distance)
- Assigned each observatory to closest city `assumption made each observatory is tied to only one city`
- Calculate City wise Mean and median
- Write final file format

### Instructions to run the program
- Dependencies
  `pip install -r requirements.txt`
- files are sourced and stored as per definition in `config/filestore.json`
- run below from terminal
   `python app.py 'DATE' `

### Opportunities
- Current final output just measures mean and median for a date it can be for the month / week
- Each observatory recording may help multiple closer cities
- Advanced imputation techniques can be used for imputing missing values and outliers
- Trends can be analyzed on population in summer and winter if additional data is given 

### calculating distance between two gps locations
https://codeburst.io/calculate-haversine-distance-between-two-geo-locations-with-python-439186315f1b