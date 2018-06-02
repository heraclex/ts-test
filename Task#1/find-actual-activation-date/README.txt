# Strategy && algorithm to deal with this problem can be done with 2 main steps:
    Step 1: Splitting large dataset into smaller pieces by grouping the phone number.
    Step 2: Finding real activation date on each group by following these sequence:
                - Sorting the records descending by activation date.
                - Find the first interruption point which is r[i].activaionDate != r[i-1].deActivationDate.
                - Then r[i].activationDate is the real one of current owner on this phone number.

# Algorithm Complexity explain:
    - Let's call:
        N: input data size.
        n: average data size on each group (after apply 'groupBy' )
        G: total number of group, or phone number. => G=N/n
        C: Spark Cluster size, or more precise, it should be number of Executor on cluster.

    - In Each Group:
        ->Sorting: Because Spark use TimSort, so in the worst case, it will take O(n*log(n)) time on 'n' elements.
        ->Finding real activation date cost: O(n).
        ==> Total cost: O(n + n*log(n)) = O(n*(1+log(n)))
    - On all group:
        ==> Total cost should be : O(G) * O(n*(1+log(n)))
                                => O((N/n) * n*(1+log(n)))
                                => O(N * (1 + log(n)))
    - On cluster:
        ==> Total cost: O((N*(1+log(n)))/C)

# Project environment:
- IntelliJ Comunity 2018
- Scala 2.11.8
- Spark 2.3.0 with Hadoop 2.8.3 on WINDOWS 10
- SBT 1.1.6 with sbt-assembly plugin

# Run App
- Build Project with sbt-assembly:     'sbt clean assembly'

- Submit Application to Spark Container by using command:

    spark-submit --class {mainClassName} --master local[{numberOfExecutor}] {fat-jar-file-path} {inputfilepath}

    =>  WHERE:
        mainClassName       => 'org.toan.assignment.FindActualActivationDateApp'
        numberOfExecutor    => 1,2,3 ... n
        fat-jar-file-path   => '\target\scala-2.11\find-actual-activation-date-0.1.0-SNAPSHOT-with-dependencies.jar'
        inputfilepath       => '\resources\inputfile.csv'

example:
spark-submit --class org.toan.assignment.FindActualActivationDateApp --master local[4] .\target\scala-2.11\find-actual-activation-date-0.1.0-SNAPSHOT-with-dependencies.jar .\resources\inputdata.csv







