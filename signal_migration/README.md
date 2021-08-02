# How To Run

Run total_migration.py with the CSV file as the first argument and an optional t as the second argument. The optional t signifies that the csv is formated with tenant IDs only with the goal of processing all datastreams in the account. 


The CSV should be formatted with the headers 'Tenant' and 'Datastream' for processing specific datastreams or just 'Tenant' if using the t flag to process all datastreams in the given accounts.


CLI Example(Datastreams given in CSV): python3 .../total_migration.py .../SmallTest.csv.  
CLI Example(Datastreams not given in CSV): python3 .../total_migration.py .../SmallTest.csv t
