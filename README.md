
# Instructions #
To run the App, type the following command in the command line:
$ sbt "run <solution number [1-5]> <path/to/resources>”
eg.
sbt "run 1 /Users/williamtun/Documents/Code/Job_Assessments/convex2/ScalaSparkDataEngineering/src/main/resources/"

# Refactor: #
1. Objects and case classes are CamelCase & methods are lowerCamelCase
2. Multiple Files read in as a STREAM (main/scala/data/DataHandlers)
3. Each row is split by "," or "\t" and values put into a case class: KeyVal(key: Int, value: Int)
4. solution choice determined by if else statements, conditioned on parameter
5. solve() method in main/scala/Solution uses context bound syntax
   and abstract class solution[A] has an additional writeData method

# Improvements #
1. use MONIX library to hook this up to an AWS bucket.
