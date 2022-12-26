# BigDataFlights

### Instructions on how to compile and execute the application

The first requirement to compile and execute the application is to have sbt installed and to be able to use the "sbt" command in the Command Line Interface. Here you can find a tutorial of how to install sbt: [sbt_instalation_guide](https://www.scala-sbt.org/1.x/docs/Setup.html), although you can use any other available on the Internet.

Once sbt is installed, the next step is to open the terminal and move to the project's main folder called “BigDataFlights”.  At this point  we should be able to see an architecture like: “/src/main/scala/Main.scala”.

Now we can compile the application executing the command:  **sbt compile**

When the compilation is done, we are ready to run the application. At this point we have to choose between executing with or without arguments. 

* Executing with arguments: 

* * Command: **sbt “run path_to_the_directory“**
  * Explanation: The application will search for the csv files in the directory that we have indicated in the arguments. path_to_the_directory must be replaced with your real path.
  * NOTICE that the quotation marks (“ ”) are important for the correct execution of the command.
Executing without arguments:

* Executing with arguments:

  * Command: **sbt run**
  * Explanation: The application will search for the csv files in the current directory where we are running the application.


Alternatively, we can execute these commands from within the sbt console. To do this the first step would be to execute: 

**sbt**

Now we would be in the sbt console and we can directly execute:

1º **build**

2º **run or run path_to_the_directory**

### Instructions on where to place the input data

All input data must be included in a single folder, containing all the .csv files that form the dataset. Also its path would be provided to the application by an argument.

However, these files can also be placed in the main folder of the project “BigDataFlights” and the application will gather the files from this location if no argument is passed.

In both types, we handle the process of reading covering us against failures. To do so, we check if the path of the data directory is valid and it is not empty. Additionally, only the csv files will be selected. If some of these requirements are not met we avoid creating an empty dataframe and an error will be prompted to indicate that it was not possible to correctly read the files.
