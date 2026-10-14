package demo.platform.repair;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class RepairCommandRunner {
    public void execute(String command, Path output) throws Exception {
        Process process = new ProcessBuilder(arguments(command))
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
        try {
            if (!process.waitFor(15, TimeUnit.MINUTES))
                throw new TimeoutException("CLI timeout; remote repair may continue");
            if (process.exitValue() != 0)
                throw new IllegalStateException("Repair CLI failed; inspect " + output);
        } finally {
            if (process.isAlive()) process.destroyForcibly();
        }
    }

    static List<String> arguments(String command) {
        List<String> arguments = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        char quote = 0;
        boolean started = false;
        for (char character : command.toCharArray()) {
            if (quote != 0) {
                if (character == quote) quote = 0;
                else current.append(character);
            } else if (character == '\'' || character == '"') {
                quote = character;
                started = true;
            } else if (Character.isWhitespace(character)) {
                if (started) {
                    arguments.add(current.toString());
                    current.setLength(0);
                    started = false;
                }
            } else {
                current.append(character);
                started = true;
            }
        }
        if (quote != 0) throw new IllegalArgumentException("Unclosed quote in repair command");
        if (started) arguments.add(current.toString());
        if (arguments.isEmpty()) throw new IllegalArgumentException("Empty repair command");
        return List.copyOf(arguments);
    }
}
