# ETL of Metrics of Brazilian E-Commerce Public Dataset

The ideia of this project is implement a ETL using this Dataset:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv

The ideia is to use the OO paradigma, to get a raw data, and results of csv file's, that represent metric of the dataset:

### time_to_approved.cvs:
This metric is the time to a buy to be appoved, this could have bring insight to try understand if there some boodleneck in this operation.

### comparation_time.csv:
This metric is the show the difference between the estimative delivered data and the real data of the delivered, the intent of this metric, is to bring a more realistic delivered data to the custumer.

### transit_time.csv:

The transit time, is the time that a product is on transit. 

### total_time.csv:

This is the metric that cout all the time, from the time to the order is made until the product is delivered in the costumer home. 

## How to used:

To use this code is necessary to have the same enviroment, for this reason there is a requirements.txt file to any user have the same enviroment.

This code use the argparse, for this reason, to run the code is necessary put more information in to run. Example:

python3 main.py <input_file> <output_path> <metric>

the value of metric can be a especific metric or all of then:
metric= 

[

'time_to_approved'

'transit_time',

'comparation_time',

'total_time',

'all'

]
