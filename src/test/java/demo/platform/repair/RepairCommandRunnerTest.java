package demo.platform.repair;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RepairCommandRunnerTest {
    @Test
    void preservesQuotedExecutableAndLiteralWindowsPathsWithoutShellExpansion() {
        assertEquals(List.of("C:\\Program Files\\tool.exe", "--target", "node one", "C:\\logs\\x.txt"),
            RepairCommandRunner.arguments("\"C:\\Program Files\\tool.exe\" --target 'node one' C:\\logs\\x.txt"));
    }

    @Test
    void rejectsMalformedCommands() {
        assertThrows(IllegalArgumentException.class, () -> RepairCommandRunner.arguments("tool 'unclosed"));
        assertThrows(IllegalArgumentException.class, () -> RepairCommandRunner.arguments(" "));
    }
}
