package cis5550.jobs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RobotExclusionProtocol {
    private static final Pattern RULE_PATTERN = Pattern.compile("(Allow|Disallow):\\s*(.*)");

    public static boolean isUrlAllowed(String robotTxt, String url, String userAgent) {
        if (robotTxt.equalsIgnoreCase("NaN")) {
            return true;
        }
        List<String> hostSpecificRules = extractRelevantLines(robotTxt, userAgent);
        if (!hostSpecificRules.isEmpty()) { // prioritize user-agent specific rules
            return evaluateRobotRules(hostSpecificRules, url);
        }
        List<String> genericRules = extractRelevantLines(robotTxt, "*");
        if (!genericRules.isEmpty()) { // check for generic rules
            return evaluateRobotRules(genericRules, url);
        }
        return true; // if no generic / user-agent specific rules; then assume true
    }

    private static List<String> extractRelevantLines(String robotTxt, String userAgent) {
        boolean isRelevantSection = false;
        String[] lines = robotTxt.split("\n");
        List<String> relevantLines = new ArrayList<String>();

        for (String line : lines) {
            line = line.trim();
            if (line.toLowerCase().startsWith("user-agent:")) {
                String userAgentInBody = line.substring("user-agent:".length()).trim();
                isRelevantSection = (userAgentInBody.equalsIgnoreCase(userAgent) && relevantLines.isEmpty());
            } else if (isRelevantSection && (line.startsWith("Allow:") || line.startsWith("Disallow:"))) {
                relevantLines.add(line);
            }
        }
        return relevantLines;
    }

    private static boolean evaluateRobotRules(List<String> rules, String url) {
        boolean isAllowed = true;
        for (String line : rules) {
            Matcher matcher = RULE_PATTERN.matcher(line);
            if (matcher.matches()) {
                String ruleType = matcher.group(1);
                String pattern = convertPathToRegex(matcher.group(2));
                if (url.matches(pattern)) {
                    isAllowed = "Allow".equalsIgnoreCase(ruleType);
                    return isAllowed;
                }
            }
        }
        return isAllowed;
    }

    private static String convertPathToRegex(String path) {
        return "^" + path.trim()
                        .replace("*", ".*")
                        .replace("?", "\\?")
                        .replaceAll("[.]", "\\.")
                        + ".*$";
    }
}
