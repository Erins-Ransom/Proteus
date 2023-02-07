#!bin/bash

# for key_length in 32 64
cd /Users/lostrong/diffident-paper/workload_gen/
make

for key_length in 64
do
	# for data_dist_type in uniform normal zipfian
	for data_dist_type in uniform
	do
		# for query_dist_type in adversary uniform normal zipfian
		for query_dist_type in correlated 
		do
			 #for max_range_size in 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
			for max_range_size in 4096
			do
				for ratio in 0.5
				do
					# for no_of_keys in 10000 100000 1000000 10000000 100000000
					for no_of_keys in 1000000
					do
						for correlation_degree in 64
						do
							cd /Users/lostrong/diffident-paper/workload_gen/
							./workload $no_of_keys $key_length 100000 $max_range_size $ratio $data_dist_type $query_dist_type $correlation_degree

							# echo "SuRF-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000; max_range: $max_range_size; point range ratio: $ratio"
							 #/Users/lostrong/diffident-paper/SuRF_standalone/SuRF/bench/my_workload SuRF 1 mixed 10 0 randint range uniform

							# echo "SuRFHash-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000; max_range: $max_range_size; point range ratio: $ratio"
							# /Users/lostrong/diffident-paper/SuRF_standalone/SuRF/bench/my_workload SuRFHash 4 mixed 10 0 randint range uniform

							# echo "SuRFReal-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000; max_range: $max_range_size; point range ratio: $ratio"
							/Users/lostrong/diffident-paper/SuRF_standalone/SuRF/bench/my_workload SuRFReal 8 mixed 10 0 randint range uniform
						done
					done
				done
			done
		done
	done
done






