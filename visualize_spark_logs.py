import sys
import json
import urllib
import numpy as np
import matplotlib.pyplot as plt

from scipy.stats import norm
from datetime import datetime


def flatten_json(y):
    """Source: https://codereview.stackexchange.com/questions/21033/flatten-dictionary-in-python-functional-style"""

    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)

    return out


if __name__ == "__main__":
    
    history_server_name = "discus-p2irc-master"
    history_server_port = "18080"
    stage_attempt = "0"

    app_name = sys.argv[1]
    graph_title = sys.argv[2]
    graph_file_prefix = graph_title.replace(")", "").lower()

    for ch in [": ", " (", " "]:
        if ch in graph_file_prefix:
            graph_file_prefix = graph_file_prefix.replace(ch, "_")

    print(graph_file_prefix)

    stages_rest_request_url = "http://"+history_server_name+":"+history_server_port+"/api/v1/applications/"+app_name+"/stages"
    stages_rest_response = urllib.urlopen(stages_rest_request_url)
    stages_json_data = json.loads(stages_rest_response.read())

    stages_with_run_time_list = []

    for i in stages_json_data:
        stage_sumbmission_time = datetime.strptime(str(i["submissionTime"].split(".")[0]), "%Y-%m-%dT%H:%M:%S") 
        stage_completion_time = datetime.strptime(str(i["completionTime"].split(".")[0]), "%Y-%m-%dT%H:%M:%S")  
        stage_total_run_time = int((stage_completion_time - stage_sumbmission_time).total_seconds()) 
        stages_with_run_time_list.append([i["stageId"], stage_total_run_time])

    stages_with_run_time_list = np.array(sorted(filter(lambda x: x[0] != 0, stages_with_run_time_list), key=lambda s: s[1], reverse=True))

    stages_id = []

    length = len(stages_with_run_time_list)

    if length < 20:
         stages_id = sorted(stages_with_run_time_list[0:1,0])
    else: 
         stages_id = sorted(stages_with_run_time_list[0:7,0])

    #write_time_list = []
    #jvm_gc_time_list = []
    #fetch_wait_time_list = []
    #result_serialization_time_list = []
    #executor_deserialize_time_list = []

    wait_time_list = []
    run_time_list = []
    host_names_list = []

    for i in stages_id:

        tasks_rest_request_url = "http://"+history_server_name+":"+history_server_port+"/api/v1/applications/"+app_name+"/stages/"+str(i)+"/"+stage_attempt+"/taskList?length=1000&sortBy=runtime"
        tasks_rest_response = urllib.urlopen(tasks_rest_request_url)
        tasks_json_data = json.loads(tasks_rest_response.read())
        tasks_json_data = filter(lambda x: ("errorMessage" not in x) and (int(x["taskMetrics"]["executorRunTime"]) != 0), tasks_json_data)
        #print(json.dumps(tasks_json_data, indent=4, sort_keys=True))

        flatten_json_data = sorted(flatten_json(tasks_json_data).items(), key=lambda s: int(s[0].split("_")[0]))

        tasks_dic = {}

        for j in flatten_json_data:
            task_index = "stage_"+str(i)+"_task_"+j[0].split("_")[0]
            metric_name = j[0].split("_")[-1]
            metric_value = j[1]
       
            if task_index in tasks_dic:
                tasks_dic[task_index].append((metric_name, metric_value)) 
            else:
                tasks_dic[task_index] = [(metric_name, metric_value)]

	for key,value in sorted(tasks_dic.items(), key=lambda s: int(s[0].split("_")[1])):
		
            value = sorted(value, key=lambda x: x[0])
		
	    task_id = ""
	    host_name = ""
	    executor_id = ""
	    task_locality = ""

            bytes_read = 0
	    write_time = 0
	    result_size = 0
	    jvm_gc_time = 0
	    records_read = 0
	    bytes_written = 0
	    records_written = 0     
	    fetch_wait_time = 0
	    local_bytes_read = 0
	    remote_bytes_read = 0
	    executor_run_time = 0
	    disk_bytes_spilled = 0
	    local_blocks_fetched = 0
	    memory_bytes_spilled = 0
	    remote_blocks_fetched = 0
	    executor_deserialize_time = 0
	    result_serialization_time = 0

	    for v in value:

                if v[0] == "host":
		    host_name = v[1].split("-")[-1]+"_"+key
		elif v[0] == "executorRunTime":
		    executor_run_time = int(v[1])/1000
		elif v[0] == "taskLocality":
		    task_locality = v[1]
		elif v[0] == "executorDeserializeTime":
		    executor_deserialize_time = int(v[1])/1000
		elif v[0] == "resultSerializationTime":
		    result_serialization_time = int(v[1])/1000
		elif v[0] == "fetchWaitTime":
		    fetch_wait_time = int(v[1])/1000
		elif v[0] == "bytesWritten":
		    bytes_written = int(v[1])
		elif v[0] == "localBytesRead":
		    local_bytes_read = int(v[1])
		elif v[0] == "recordsWritten":
		    records_written = int(v[1])
		elif v[0] == "localBlocksFetched":
		    local_blocks_fetched = int(v[1])
		elif v[0] == "memoryBytesSpilled":
		    memory_bytes_spilled = int(v[1])
		elif v[0] == "diskBytesSpilled":
		    disk_bytes_spilled = int(v[1])
		elif v[0] == "remoteBytesRead":
		    remote_bytes_read = int(v[1])
		elif v[0] == "recordsRead":
		    records_read = int(v[1])
		elif v[0] == "executorId":
		    executor_id = str(v[1])
		elif v[0] == "bytesRead":
		    bytes_read = int(v[1])
		elif v[0] == "taskId":
		    task_id = str(v[1])
		elif v[0] == "writeTime":
		    write_time = int(v[1])/1000
		elif v[0] == "resultSize":
		    result_size = int(v[1])
		elif v[0] == "jvmGcTime":
		    jvm_gc_time = int(v[1])/1000
		elif v[0] == "remoteBlocksFetched":
		    remote_blocks_fetched = int(v[1])

            host_names_list.append([host_name])
            run_time_list.append([executor_run_time])
            wait_time_list.append([executor_deserialize_time+result_serialization_time+fetch_wait_time+jvm_gc_time])

    #print("Stages: " + str(stages_id))
    #print("Number of Tasks: " + str(len(run_time_list)))

    host_names_array = np.array(host_names_list)
    run_time_array = np.array(run_time_list)
    wait_time_array = np.array(wait_time_list)

    plot_data = np.concatenate((host_names_array, wait_time_array, run_time_array), axis=1)
    plot_data = np.array(sorted(list(plot_data), key=lambda x: int(x[2])))

    hosts = np.array(plot_data[0:,0])
    wait_time = np.array(plot_data[0:,1], dtype=np.int)
    run_time = np.array(plot_data[0:,2], dtype=np.int)

    run_time_mean = np.mean(run_time)
    run_time_stdev = np.std(run_time)

    run_time_cdf = norm.cdf(run_time, run_time_mean, run_time_stdev)
    run_time_pdf = norm.pdf(run_time, run_time_mean, run_time_stdev)

    fig, ax = plt.subplots(figsize=(15, 10))
    plt.subplots_adjust(left=0.14, bottom=0.09, right=0.98, top=0.95, wspace=0.3, hspace=0.3)
    plt.xticks(fontsize=20)

    y_pos = np.arange(len(hosts))

    plot_1 = ax.barh(y_pos, run_time, align="center", color="gray")
    plot_2 = ax.barh(y_pos, wait_time, align="center", color="darkred")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(hosts)
    ax.invert_yaxis()  
    ax.set_xlabel("Run time (s)", fontsize=25)
    ax.set_title(graph_title, fontsize=25)
    ax.set_ylim(len(hosts)-0.5, -0.6)
    ax.set_xlim(0, max(run_time)+5)
    plt.legend((plot_1[0], plot_2[0]), ("Executor Run Time", "Executor Wait Time"), fontsize=15)
    plt.savefig(graph_file_prefix+"_execution_time.pdf", format="pdf", dpi=10000)


    fig, ax = plt.subplots(figsize=(15, 10))
    plt.subplots_adjust(left=0.11, bottom=0.11, right=0.98, top=0.88, wspace=None, hspace=None)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    plt.plot(run_time, run_time_pdf, "*-")
    #plt.title("Probability Density Function", fontsize=25)
    plt.xlabel("Run time (s)", fontsize=25)
    plt.ylabel("PDF", fontsize=25)
    plt.savefig(graph_file_prefix+"_pdf.pdf", format="pdf", dpi=10000)


    fig, ax = plt.subplots(figsize=(15, 10))
    plt.subplots_adjust(left=0.07, bottom=0.11, right=0.98, top=0.88, wspace=None, hspace=None)
    plt.xticks(fontsize=20)
    plt.yticks(fontsize=20)

    plt.plot(run_time, run_time_cdf, "*-")
    #plt.title("Cumulative Distribution Function", fontsize=25)
    plt.xlabel("Run time (s)", fontsize=25)
    plt.ylabel("CDF", fontsize=25)
    plt.savefig(graph_file_prefix+"_cdf.pdf", format="pdf", dpi=10000)

    plt.show()



