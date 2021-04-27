package com.telespazio.hsaf.scavenger;

import java.io.File;

import picocli.CommandLine;

/**
 * Command line arguments.
 * <p>
 * Makes use of picocli library annotations.
 *
 * @author Alessandro Falappa
 */
@CommandLine.Command(
        name = "java -jar scavenger-#.#.jar",
        synopsisHeading = "Usage:%n    ",
        optionListHeading = "Options:%n",
        parameterListHeading = "Arguments:%n"
)
public class CmdLineArgs {

    @CommandLine.Parameters(paramLabel = "CONFIG_FILE", description = "Path of configuration file")
    File configFile = null;

    @CommandLine.Option(names = "-v", description = "Print what the program is doing in more detail.")
    boolean verbose = false;
}
