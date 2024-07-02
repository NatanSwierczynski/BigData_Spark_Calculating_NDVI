# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/


###     odpalanie klastra na sparku     ###

#62.87.248.81
#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 156.17.227.50
#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 62.87.248.81
#./sbin/spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 62.87.248.81
#org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 62.87.248.81

#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 192.168.1.192
#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 192.168.1.254


#./bin/spark-class org.apache.spark.deploy.worker.Worker
#spark://host1:port1


#192.168.1.192

#192.168.1.254

#http://localhost:4040/
#http://localhost:8080/

#192.168.50.34

#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 192.168.50.34
#spark-class org.apache.spark.deploy.worker.Worker spark://156.17.227.50:7077 --host 81.161.203.224
#81.161.203.224

#log
#C:\Spark\spark-3.3.1-bin-hadoop3\work