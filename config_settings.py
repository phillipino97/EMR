configurations = {
    'm5.xlarge': [
        {
           "Classification": "mapred-site",
           "Properties": {
               "mapreduce.framework.name": "yarn",
               "mapred.tasktracker.map.tasks.maximum": "2",
               "mapreduce.map.java.opts": "-Xmx2458m",
               "mapreduce.reduce.java.opts": "-Xmx4916m",
               "mapreduce.map.memory.mb": "3072",
               "mapreduce.reduce.memory.mb": "6144",
               "yarn.app.mapreduce.am.resource.mb": "6144",
               "yarn.scheduler.minimum-allocation-mb": "32",
               "yarn.scheduler.maximum-allocation-mb": "12288",
               "yarn.nodemanager.resource.memory-mb": "12288"
           }
        },
        {
            "Classification": "hadoop-env",
            "Properties": {},
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "YARN_RESOURCEMANAGER_HEAPSIZE": "2416",
                        "YARN_PROXYSERVER_HEAPSIZE": "2416",
                        "YARN_NODEMANAGER_HEAPSIZE": "2048",
                        "HADOOP_JOB_HISTORYSERVER_HEAPSIZE": "2416",
                        "HADOOP_NAMENODE_HEAPSIZE": "1843",
                        "HADOOP_DATANODE_HEAPSIZE": "778",
                        "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"
                    }
                }
            ]
        }
    ],
    'm1.medium': [
        {
           "Classification": "mapred-site",
           "Properties": {
               "mapreduce.framework.name": "yarn",
               "mapred.tasktracker.map.tasks.maximum": "2",
               "mapreduce.map.java.opts": "-Xmx512m",
               "mapreduce.reduce.java.opts": "-Xmx768m",
               "mapreduce.map.memory.mb": "768",
               "mapreduce.reduce.memory.mb": "1024",
               "yarn.app.mapreduce.am.resource.mb": "1024",
               "yarn.scheduler.minimum-allocation-mb": "256",
               "yarn.scheduler.maximum-allocation-mb": "2048",
               "yarn.nodemanager.resource.memory-mb": "2048"
           }
        },
        {
            "Classification": "hadoop-env",
            "Properties": {},
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "YARN_RESOURCEMANAGER_HEAPSIZE": "384",
                        "YARN_PROXYSERVER_HEAPSIZE": "192",
                        "YARN_NODEMANAGER_HEAPSIZE": "256",
                        "HADOOP_JOB_HISTORYSERVER_HEAPSIZE": "256",
                        "HADOOP_NAMENODE_HEAPSIZE": "384",
                        "HADOOP_DATANODE_HEAPSIZE": "192",
                        "HADOOP_NAMENODE_OPTS": "-XX:GCTimeRatio=19"
                    }
                }
            ]
        }
    ]
}