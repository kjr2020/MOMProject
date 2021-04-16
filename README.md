# MOMProject

This Codes for Experiment of Performance Comparison with MOMs which called RabbitMQ, Apache Kafka, Apache ActiveMQ
in MTC(Many Task Computing) in distributed environment.

I have three nodes (master, slave1, slave2) and 3,000 tasks in workload that choose numbers in 1 ~ 99 randomly is execute time.

Master is 1 producer and 2 executors, slave1 and slave2 is no producer and 4 executors each.

Then, We have 10 executors and 1 producer.

Producer get tasks in 'sleeptask' file and Producing them in Queue or Topic.

Consumer get those tasks to Queue or Topic and wait in as much as task's value and when all tasks consumed It will be closed there connections

I want to compare to three MOM's(RabbitMQ, Kafka, ActiveMQ) Producer's execute time, Consumer's execute time and Load Balancing.

It will be alert that which MOM is best option for MTC in Distribute Computing.

I provide two version of codes Java and Python but, I think ActiveMQ in Python using STOMP is unstable and occurred some problems in performance
Then I didn't provide
