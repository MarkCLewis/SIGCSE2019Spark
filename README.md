# SIGCSE 2019 Big Data Analytics with Spark Workshop

Repository for all my examples and instructions for a workshop on Spark at 
SIGCSE 2019. In order to run this, all you really need is 
[sbt](https://www.scala-sbt.org/). If you want an IDE to code in, you can use
[Eclipse](http://scala-ide.org/), [IntelliJ](https://www.jetbrains.com/help/idea/discover-intellij-idea-for-scala.html), or [VS Code](https://marketplace.visualstudio.com/items?itemName=scalameta.metals).
These tools run on Windows, Mac, and Linux. If you are on a Window machine and
want to use Eclipse, I recommend using [MinGW](http://www.mingw.org/) as your command-line and not
the Windows/Ubuntu Bash extension because Bash uses a different path
configuration when you run "sbt compile eclipse" in the project to create the
Eclipse project. For my students I often recommend they install [SourceTree](https://www.sourcetreeapp.com/)
and use the terminal in there as that gives them MinGW and git working happily on a Windows machine.

The slides for the presentation are on [Google Docs](https://docs.google.com/presentation/d/1p2RpoDN02OkdN43hrGGywir_3Uc2o3DdHk9jet65wC8/edit?usp=sharing).

There are three data sets used in the examples: [Adult Census Income Data](https://www.kaggle.com/uciml/adult-census-income), 
[BRFSS Health Survey](https://www.cdc.gov/brfss/) and [Marvel Social Network](https://www.kaggle.com/csanhueza/the-marvel-universe-social-network).
