#!bin/bash
for data_dist_type in uniform normal zipfian
do
	for query_dist_type in uniform normal zipfian adversary
	# for query_dist_type in uniform
	do
		for no_of_keys in 10000 100000 1000000 10000000 100000000 1000000000
		do
			for key_length in 32 64
			do

				cd /Users/lostrong/diffident-paper/workload_gen/
				./workload $no_of_keys $key_length 1000000 $data_dist_type $query_dist_type

				echo "SuRF-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000"
				/Users/lostrong/Dropbox/Research/ongoing/DST/SuRF/build/bench/my_workload SuRF 1 mixed 10 0 randint range uniform

				echo "SuRFHash-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000"
				/Users/lostrong/Dropbox/Research/ongoing/DST/SuRF/build/bench/my_workload SuRFHash 4 mixed 10 0 randint range uniform

				echo "SuRFReal-- data_dist_type: $data_dist_type; query_dist_type: $query_dist_type; key_length: $key_length; no_entries: $no_of_keys; no_of_queries: 1000000"
				/Users/lostrong/Dropbox/Research/ongoing/DST/SuRF/build/bench/my_workload SuRFReal 4 mixed 10 0 randint range uniform


			done
		done
	done
done






