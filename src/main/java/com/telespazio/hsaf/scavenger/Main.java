package com.telespazio.hsaf.scavenger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPException;
import it.sauronsoftware.ftp4j.FTPFile;
import it.sauronsoftware.ftp4j.FTPIllegalReplyException;
import picocli.CommandLine;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.Locale.US;

/**
 * Application main class.
 *
 * @author Alessandro Falappa
 */
public class Main {

    private static final SimpleDateFormat sdfOutput = new SimpleDateFormat("MMM dd HH:mm", US);
    private static final SimpleDateFormat sdfLog = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String PRODUCT_YEAR = "2018";
    
    private static Comparator<FTPFile> ftpFileDateComparator = new Comparator<FTPFile>() {
        @Override
        public int compare(FTPFile o1, FTPFile o2) {
            return o1.getModifiedDate().compareTo(o2.getModifiedDate());
        }
    };
    private static final CmdLineArgs cla = new CmdLineArgs();

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine(cla);
        try {
            // parse command line arguments
            cmdLine.parse(args);
        } catch (CommandLine.ParameterException e) {
            // command line arguments parsing error
            System.err.println(e.getMessage());
            printUsage(cmdLine);
            System.exit(1);
        }
        // check filepaths
        if (cla.configFile != null) {
            if (!cla.configFile.canRead()) {
                System.err.format("Couldn't find/read file '%s'%n", cla.configFile);
                printUsage(cmdLine);
                System.exit(1);
            }
        }
        // read configuration file
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(cla.configFile));
        } catch (IOException ex) {
            System.err.format("I/O problem: %s%n", ex.toString());
        }
        // start processing
        if (!props.isEmpty()) {
            System.out.println("============   Monitor Scavenger   ============");
            System.out.format("Started on %s%n", sdfLog.format(new Date()));
            int numRetries = Integer.parseInt(props.getProperty("ftp.retry"));
            int waitSecs = Integer.parseInt(props.getProperty("ftp.retry.wait-secs"));
            boolean allOk = false;
            int currentTry = 0;
            do {
                try {
                    String hostname = props.getProperty("ftp.host");
                    String user = props.getProperty("ftp.user");
                    String pw = props.getProperty("ftp.password");
                    if (cla.verbose) {
                        System.out.format("Connecting to '%s' as '%s'%n", hostname, user);
                    }
                    FTPClient ftp = new FTPClient();
                    ftp.connect(hostname);
                    if (ftp.isConnected()) {
                        ftp.login(user, pw);
                        if (ftp.isAuthenticated()) {
                            scavenge(ftp, props, numRetries, waitSecs);
                        } else {
                            System.err.println("Invalid credentials. Username or password not accepted by remote site");
                        }
                        ftp.disconnect(true);
                        allOk = true;
                    } else {
                        System.err.println("Couldn't connect to remote site");
                    }
                } catch (IllegalStateException ex) {
                    System.err.format("Illegal FTP state: %s%n", ex.toString());
                } catch (IOException ex) {
                    System.err.format("I/O problem: %s%n", ex.toString());
                } catch (FTPIllegalReplyException ex) {
                    System.err.format("Illegal FTP reply: %s%n", ex.toString());
                } catch (FTPException ex) {
                    System.err.format("FTP problem: %s%n", ex.toString());
                }
                if (!allOk) {
                    currentTry++;
                    if (currentTry < numRetries) {
                        try {
                            if (cla.verbose) {
                                System.out.format("Waiting %d seconds before retrying. %d retries left%n", waitSecs,
                                        numRetries - currentTry);
                            }
                            Thread.sleep(1000 * waitSecs);
                        } catch (InterruptedException ex) {
                            // ignored
                        }
                    }
                }
            } while (!allOk && currentTry < numRetries);
        }
        System.out.format("Finished on %s%n", sdfLog.format(new Date()));
    }

    private static void printUsage(CommandLine cmdLine) {
        System.out.println();
        cmdLine.usage(System.out);
    }

    private static void scavenge(FTPClient ftp, Properties props, int numRetries, int waitSecs) {
        long millis = System.currentTimeMillis();
        int c = 1;
        String prodName = props.getProperty(String.format("%d.product-name", c));
        boolean removeFirst = Boolean.parseBoolean(props.getProperty("status-files.remove-first"));
        while (prodName != null) {
            long prodMillis = System.currentTimeMillis();
            int timeWindow = Integer.parseInt(props.getProperty(String.format("%d.time-window.hours", c)));
            String remoteDir = props.getProperty("ftp.basedir").concat(props.getProperty(String.format("%d.ftp.subdir", c)));
            String outDir = props.getProperty("reports.outdir");
            System.out.println("------------");
            System.out.format("Product: %s%n", prodName);
            if (cla.verbose) {
                System.out.format("Time window: %d hours%n", timeWindow);
                System.out.format("Remote dir: %s%n", remoteDir);
            }
            scavengeProduct(ftp, prodName, timeWindow, remoteDir, outDir, numRetries, waitSecs, removeFirst);
            if (cla.verbose) {
                prodMillis = System.currentTimeMillis() - prodMillis;
                System.out.format("'%s' scavenging took %,d msecs%n", prodName, prodMillis);
            }
            c++;
            prodName = props.getProperty(String.format("%d.product-name", c));
        }
        System.out.println("------------");
        millis = System.currentTimeMillis() - millis;
        System.out.format("Whole scavenging took %,d msecs%n", millis);
    }

    private static void scavengeProduct(FTPClient ftp, String prodName, int timeWindow, String remoteDir, String outDir, int numRetries, int waitSecs, boolean removeFirst) {
        Path statusFile = Paths.get(outDir, String.format("%s.status", prodName));
        Path statusFileTmp = Paths.get(outDir, String.format("%s.status.tmp", prodName));
        Path okFile = Paths.get(outDir, String.format("%s.report.ok", prodName));
        Path nokFile = Paths.get(outDir, String.format("%s.report.nok", prodName));
        boolean scavengeOk = false;
        boolean finishRetrying = false;
        int currentTry = 0;
        do {
            try {
                // calculate start time
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.HOUR, -timeWindow);
                Date startTime = cal.getTime();
                if (cla.verbose) {
                    System.out.format("Extracting products since: %s%n", sdfLog.format(startTime));
                }
                // collect info for files modified after start time
                ftp.changeDirectory(remoteDir);
                FTPFile[] files = ftp.list();
                List<FTPFile> extractedFiles = new ArrayList<>();
                for (FTPFile file : files) {
                    if (file.getType() == FTPFile.TYPE_FILE) {
                        Date modDate = file.getModifiedDate();
                        if (modDate.after(startTime)) {
                            extractedFiles.add(file);
                        }
                    }
                }
                System.out.format("Extracted %d products%n", extractedFiles.size());
                // sort extracted files by date
                if (cla.verbose) {
                    System.out.println("Sorting by modification date");
                }
                Collections.sort(extractedFiles, ftpFileDateComparator);
                List<String> reportLines = new LinkedList<>();
                for (FTPFile file : extractedFiles) {
                    reportLines.add(String.format("%d %s %s", file.getSize(), sdfOutput.format(file.getModifiedDate()), file.
                            getName()));
                }
                // remove previous status file as per configuration
                try {
                    if (removeFirst) {
                        if (cla.verbose) {
                            System.out.println("Removing previous status file");
                        }
                        Files.deleteIfExists(statusFile);
                    }
                } catch (IOException ex) {
                    // ignored
                }
                // wrte status file
                if (!reportLines.isEmpty()) {
                    if (cla.verbose) {
                        System.out.format("Writing status: %s%n", statusFile.toString());
                    }
                    // safe writing (write to tmp file then rename)
                    Files.write(statusFileTmp, reportLines, defaultCharset());
                    Files.move(statusFileTmp, statusFile, StandardCopyOption.REPLACE_EXISTING);
                    scavengeOk = true;
                }
                finishRetrying = true;
            } catch (Exception ex) {
                System.err.println(ex.toString());
            }
            if (!finishRetrying && !scavengeOk) {
                currentTry++;
                if (currentTry < numRetries) {
                    try {
                        if (cla.verbose) {
                            System.out.format("Waiting %d seconds before retrying. %d retries left%n", waitSecs,
                                    numRetries - currentTry);
                        }
                        Thread.sleep(1000 * waitSecs);
                    } catch (InterruptedException ex) {
                        // ignored
                    }
                }
            }
        } while (!finishRetrying && currentTry < numRetries);
        // remove previous ok/nok files
        try {
            if (cla.verbose) {
                System.out.println("Removing previous ok/nok files");
            }
            Files.deleteIfExists(okFile);
            Files.deleteIfExists(nokFile);
        } catch (IOException ex) {
            // ignored
        }
        // write out ok or nok file depending on outcome
        if (scavengeOk) {
            // write out ok file
            if (cla.verbose) {
                System.out.format("Writing ok file: %s%n", okFile.toString());
            }
            try {
                Files.createFile(okFile);
            } catch (IOException ex) {
                // ignored
            }
        } else {
            try {
                // NOTE: leftover status file is not deleted to intentionally maintain last good file list
                // write out nok file
                if (cla.verbose) {
                    System.out.format("Writing nok file: %s%n", nokFile.toString());
                }
                Files.createFile(nokFile);
            } catch (IOException ex1) {
                // ignored
            }
        }
    }

}
