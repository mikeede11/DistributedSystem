package edu.yu.cs.com3800;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        Logger log = Logger.getLogger(fileNamePreface);
        log.setUseParentHandlers(disableParentHandlers);
        log.info(log.getName());
        log.setLevel(Level.FINEST);

        FileHandler fh = null;
            fh = new FileHandler(fileNamePreface + ".log");
            log.addHandler(fh);
            fh.setLevel(Level.FINEST);
            fh.setFormatter(new SimpleFormatter());
            log.info("Log is logging to " + fileNamePreface);
            Logger log2 = log;
        return log;

    }
}
