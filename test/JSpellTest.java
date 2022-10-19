import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JSpellTest {

    private final String LOG_FORMAT = "<Date> <Time> <Pid> <Level> <Component>: <Content>";

    @Test
    void LCSMatch() {
        var spell = new JSpell();
        String[] seq1 = {"Receiving", "block", "blk_-1608999687919862906", "src", "/10.250.10.6", "40524", "dest", "/10.250.10.6", "50010"};
        String[] seq2 = {"Just", "A", "Test"};
        String[] logmessageL = {"Receiving", "block", "blk_-1608999687919862906", "src", "/10.250.19.102", "54106", "dest", "/10.250.19.102", "50010"};
        int logID = 0;
        var newCluster = new LogCluster(List.of(logmessageL), Collections.singletonList(logID));

        var retLogClust = spell.LCSMatch(Collections.singletonList(newCluster), List.of(seq1));
        assertEquals(newCluster.getTemplate(), retLogClust.getTemplate());

        var ret = spell.LCSMatch(Collections.singletonList(newCluster), List.of(seq2));
        assertEquals(ret, null);
    }

    @Test
    void LCS() {
        var spell = new JSpell();
        String[] seq1 = {"Receiving", "block", "blk_-1608999687919862906", "src", "/10.250.10.6", "40524", "dest", "/10.250.10.6", "50010"};
        String[] seq2 = {"Receiving", "block", "blk_-1608999687919862906", "src", "/10.250.19.102", "54106", "dest", "/10.250.19.102", "50010"};
        String[] expected_lcs = {"Receiving", "block", "blk_-1608999687919862906", "src", "dest", "50010"};

        var lcs = spell.LCS(List.of(seq1), List.of(seq2));
        assertEquals(List.of(expected_lcs), lcs);
    }

    @Test
    void simpleLoopMatch() {
    }

    @Test
    void loadDataframe() {
    }

    @Test
    void generateLogformatRegex() {
        var spell = new JSpell();
        Set<String> expected_header = new HashSet<>(List.of(new String[]{"Date", "Time", "Pid", "Level", "Component", "Content"}));
        var expected_regex = Pattern.compile(
                "^(?<Date>.*?) (?<Time>.*?) (?<Pid>.*?) (?<Level>.*?) (?<Component>.*?): (?<Content>.*?)$"
        );

        spell.generateLogformatRegex(LOG_FORMAT);
        var regex = spell.regex;
        assertEquals(expected_header, spell.dataframe.keySet());
        assertEquals(expected_header.size(), spell.dataframe.keySet().size());
        assertEquals(expected_regex.toString(), regex.toString());
    }
}