#!bin/bash

echo "**************** HERE ************************"
python gen_load.py randint uniform 
echo "**************** HERE 1 ************************"
python gen_txn.py randint uniform
echo "**************** HERE 2 ************************"
python gen_txn.py randint zipfian
echo "**************** HERE 3 ************************"
#python gen_txn.py randint latest

#python gen_load.py email uniform
#python gen_txn.py email uniform
#python gen_txn.py email zipfian
#python gen_txn.py email latest


