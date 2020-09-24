The key improvements are: 
1. In the scala/logic folder are 2 additional solutions to the task. 
"solution4" uses RDD and "solution5" applies tail recursion 
2. An encapsulating "solution" abstract class has been added to implicitly detect the data type of the input into a solve function. The solve function that applies the appropriate solution depending on the input data type. 
3. I've added a variety of unit and functional tests. I decided to use several different styles of test just to showcase i am aware of the variety of styles that exist. 
4. ReadAllFiles() method has been added which loops through ALL the files in a given folder. 
  Thus the business logic can now be applied across the list of files (csv or tsv) and written out respectively. 

The recruiter advised I upload my work by wednesday evening 
in case another candidate gets the spot. 
 
Still, had I even more time, I would: 
1. Get this code working on the commandline 
   and add a parameter to toggle between solutions
2. Add try-catch statements to handle 
   cases where no files are found or inappropriate file types are found
3. use MONIX library to hook this up to an AWS bucket. 
