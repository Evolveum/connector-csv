package com.evolveum.polygon.connector.csv.util;


import com.evolveum.polygon.connector.csv.CsvConfiguration;
import com.evolveum.polygon.connector.csv.CsvConnector;
import com.evolveum.polygon.connector.csv.ObjectClassHandlerConfiguration;
import com.evolveum.polygon.connector.csv.util.Util.ParametersForDiscovery;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.SuggestedValues;
import org.identityconnectors.framework.common.objects.SuggestedValuesBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

import static com.evolveum.polygon.connector.csv.CsvConfiguration.*;
import static com.evolveum.polygon.connector.csv.CsvConfiguration.PROP_FIELD_DELIMITER;
import static com.evolveum.polygon.connector.csv.CsvConfiguration.PROP_MULTIVALUE_DELIMITER;

public class ConfigurationDetector {

    private static final Log LOG = Log.getLog(ConfigurationDetector.class);

    private static final int MAX_PROCESSED_ROW = 1000;

    private final ObjectClassHandlerConfiguration configuration;

    public ConfigurationDetector(ObjectClassHandlerConfiguration configuration) {
        this.configuration = configuration;
    }

    public Map<String, SuggestedValues> detectDelimiters() {

        Map<String, SuggestedValues> suggestions = new HashMap<>();

        try {

            List<Character> characters = readCharacters();

            Set<Character> allSymbols = new HashSet<>();
            Map<Character, SymbolWrapper> symbols = new HashMap<>();
            List<Map<Character, SymbolWrapper>> symbolsPerRow = new ArrayList<>();

            int i;
            for (i = 0; i < characters.size(); i++) {

                char ch = characters.get(i);

                if (isSymbol(ch)) {
                    if (Util.UTF8_BOM.charAt(0) != ch) {
                        allSymbols.add(ch);
                    }
                    increment(symbols, ch, isFirstInRow(i, characters));
                } else if (isNewLineChar(ch) && !symbols.isEmpty()) {
                    symbolsPerRow.add(symbols);
                    symbols = new HashMap<>();
                }
            }

            if (!symbols.isEmpty() && i == characters.size()) {
                symbolsPerRow.add(symbols);
            }

            Set<Character> isNotInEveryRow = new HashSet<>();

            symbolsPerRow.forEach(currentRow -> {
                allSymbols.forEach(symbol -> {
                    if (!currentRow.containsKey(symbol)) {
                        isNotInEveryRow.add(symbol);
                    }
                });
            });

            Set<Character> commentSuggestions = createCommentSuggestions(isNotInEveryRow, symbolsPerRow);

            removeRowWithComment(symbolsPerRow, commentSuggestions);

            Set<Character> quote = createQuoteSuggestions(allSymbols, symbolsPerRow, commentSuggestions);

            Set<Character> delimiters = createRecordDelimitersSuggestions(symbolsPerRow, allSymbols);

            if (quote.contains('"') && delimiters.contains('"')) {
                delimiters.remove('"');
            }

            if (quote.contains('\'') && delimiters.contains('\'')) {
                delimiters.remove('\'');
            }

            delimiters.forEach(delimiter -> {
                if (quote.contains(delimiter)) {
                    quote.remove(delimiter);
                }
            });

            if (!commentSuggestions.isEmpty()) {
                suggestions.put(PROP_COMMENT_MARKER,
                        SuggestedValuesBuilder.buildOpen(commentSuggestions.toArray()));
            }
            if (!delimiters.isEmpty()) {
                suggestions.put(PROP_FIELD_DELIMITER,
                        SuggestedValuesBuilder.buildOpen(delimiters.toArray()));
            }

            Set<Character> multivalueDelimiters = createMultivalueDelimitersSuggestions(characters, delimiters, quote);

            if (!quote.isEmpty()) {
                suggestions.put(PROP_QUOTE,
                        SuggestedValuesBuilder.buildOpen(quote.toArray()));
            }
            if (!multivalueDelimiters.isEmpty()) {
                suggestions.put(PROP_MULTIVALUE_DELIMITER,
                        SuggestedValuesBuilder.buildOpen(multivalueDelimiters.toArray()));
            }

        } catch (IOException e) {
            LOG.error("Couldn't create reader for file: {1} ({2})", configuration.getFilePath().getPath(), e.getMessage());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        return suggestions;
    }

    private Set<Character> createRecordDelimitersSuggestions(List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> allSymbols) {
        Map<Character, Integer> sums = new HashMap<>();
        Set<Character> toRemove = new HashSet<>();

        symbolsPerRow.forEach(previousRow -> {
            symbolsPerRow.forEach((currentRow) -> {
                allSymbols.forEach(symbol -> {
                    SymbolWrapper previousWrapper = previousRow.get(symbol);
                    SymbolWrapper currentWrapper = currentRow.get(symbol);
                    if (previousWrapper == null && currentWrapper == null) {
                        toRemove.add(symbol);
                    }

                    if (previousWrapper != null && currentWrapper != null) {
                        increment(sums, symbol, Math.abs(previousWrapper.countInRow - currentWrapper.countInRow));
                    }
                });
            });
        });

        if (toRemove.size() == sums.size()) {
            Map<Character, Integer> lineCount = new HashMap<Character, Integer>();
            for (int i = 0; i < symbolsPerRow.size(); i++) {
                for (Character symbolInRow : symbolsPerRow.get(i).keySet()) {
                    Integer count = lineCount.get(symbolInRow);
                    if (count == null) {
                        count = 0;
                    }
                    lineCount.put(symbolInRow, count + 1);
                }
            }

            Integer highestLineCount = null;
            for (Map.Entry<Character, Integer> e : lineCount.entrySet()) {
                if (highestLineCount == null || highestLineCount < e.getValue()) {
                    highestLineCount = e.getValue();
                }
            }

            List<Character> bestCandidates = new ArrayList<>();
            for (Map.Entry<Character, Integer> e : lineCount.entrySet()) {
                if (e.getValue().equals(highestLineCount)) {
                    bestCandidates.add(e.getKey());
                }
            }

            if (!bestCandidates.isEmpty()) {
                toRemove.removeAll(bestCandidates);
            }
        }

        sums.keySet().removeAll(toRemove);

        return sums.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)).keySet();
    }

    private Set<Character> createMultivalueDelimitersSuggestions(List<Character> characters, Set<Character> delimiters, Set<Character> quote) {
        Map<Character, Integer> multivalueDelimiterMap = new HashMap<>();
        List<Character> processedDelimiters = new ArrayList<>();
        for (int i = 0; i < characters.size(); i++) {
            char ch = characters.get(i);
            if (delimiters.contains(ch) && i + 1 < characters.size()) {
                if (!processedDelimiters.contains(ch) && i > 0) {
                    for (int j = i - 1; j >= 0; j--) {
                        char prevCh = characters.get(j);
                        if (ch == prevCh || isNewLineChar(prevCh)) {
                            break;
                        }
                        if (isSymbol(prevCh)) {
                            if (!delimiters.contains(prevCh)
                                    && j != 0
                                    && j != (i - 1)
                                    && ch != characters.get(j - 1)
                                    && !isNewLineChar(characters.get(j - 1))) {
                                increment(multivalueDelimiterMap, prevCh, 1);

                            }
                            if (quote.contains(prevCh) && isNotQuote(j, characters)) {
                                quote.remove(prevCh);
                            }
                        }
                    }
                    processedDelimiters.add(ch);
                }
                for (int j = i + 1; j < characters.size(); j++) {
                    char nextCh = characters.get(j);
                    if (ch == nextCh || isNewLineChar(nextCh)) {
                        break;
                    }
                    if (isSymbol(nextCh)) {
                        if (!delimiters.contains(nextCh)
                                && (j + 1) != characters.size()
                                && j != (i + 1)
                                && ch != characters.get(j + 1)
                                && !isNewLineChar(characters.get(j + 1))) {
                            increment(multivalueDelimiterMap, nextCh, 1);

                        }
                        if (quote.contains(nextCh) && isNotQuote(j, characters)) {
                            quote.remove(nextCh);
                        }
                    }
                }
            }
            if (isNewLineChar(ch)) {
                processedDelimiters.clear();
            }
        }
        return multivalueDelimiterMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (oldValue, newValue) -> oldValue, LinkedHashMap::new)).keySet();
    }

    private boolean isNotQuote(int j, List<Character> characters) {
        return j != 0 && !isSymbol(characters.get(j - 1)) && !isNewLineChar(characters.get(j - 1))
                && !isSymbol(characters.get(j + 1)) && !isNewLineChar(characters.get(j + 1)) && (j + 1) != characters.size();
    }

    private Set<Character> createQuoteSuggestions(Set<Character> allSymbols, List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> commentSuggestions) {
        Set<Character> quote = new HashSet<>();

        allSymbols.forEach(symbol -> {
            boolean evenInEveryRowEven = true;
            for (Map<Character, SymbolWrapper> row : symbolsPerRow) {
                SymbolWrapper wrapper = row.get(symbol);
                if (commentSuggestions.contains(symbol) || wrapper != null && (wrapper.getCountInRow() % 2) != 0) {
                    evenInEveryRowEven = false;
                }
            }
            if (evenInEveryRowEven) {
                quote.add(symbol);
            }
        });
        return quote;
    }

    private void removeRowWithComment(List<Map<Character, SymbolWrapper>> symbolsPerRow, Set<Character> commentSuggestions) {
        Iterator<Map<Character, SymbolWrapper>> symbolsPerRowIterator = symbolsPerRow.iterator();

        while (symbolsPerRowIterator.hasNext()) {
            Map<Character, SymbolWrapper> row = symbolsPerRowIterator.next();
            commentSuggestions.forEach(commentSymbol -> {
                SymbolWrapper wrapper = row.get(commentSymbol);
                if (wrapper != null && wrapper.isFirstInRow()) {
                    symbolsPerRowIterator.remove();
                }
            });
        }
    }

    private Set<Character> createCommentSuggestions(Set<Character> isNotInEveryRow, List<Map<Character, SymbolWrapper>> symbolsPerRow) {
        Set<Character> commentSuggestions = new HashSet<>();
        isNotInEveryRow.forEach(character -> {
            boolean isFirstInEveryRow = true;
            for (Map<Character, SymbolWrapper> row : symbolsPerRow) {
                SymbolWrapper wrapper = row.get(character);
                if (wrapper != null && (wrapper.countInRow != 1 || !wrapper.isFirstInRow)) {
                    isFirstInEveryRow = false;
                }
            }
            if (isFirstInEveryRow) {
                commentSuggestions.add(character);
            }
        });
        return commentSuggestions;
    }

    private List<Character> readCharacters() throws IOException {
        BufferedReader reader = Util.createReader(configuration);
        int countOfRow = 0;

        List<Character> characters = new ArrayList<>();

        synchronized (CsvConnector.SYNCH_FILE_LOCK) {
            int r;
            boolean previousIsNewLine = false;
            while ((r = reader.read()) != -1
                    && (countOfRow < MAX_PROCESSED_ROW || (countOfRow == MAX_PROCESSED_ROW && previousIsNewLine))) {
                char ch = (char) r;
                if (isNewLineChar(ch)) {
                    if (!previousIsNewLine) {
                        countOfRow++;
                        previousIsNewLine = true;
                    }
                } else {
                    previousIsNewLine = false;
                    if (countOfRow == MAX_PROCESSED_ROW) {
                        continue;
                    }
                }
                characters.add(ch);
            }
            reader.close();
        }
        return characters;
    }

    public Map<String, ? extends SuggestedValues> detectAttributes(Map<String, SuggestedValues> suggestedDelimiters) {
        Map<String, SuggestedValues> suggestions = new HashMap<>();

        try {
            BufferedReader reader = Util.createReader(configuration);

            String firstLine;

            synchronized (CsvConnector.SYNCH_FILE_LOCK) {
                firstLine = reader.readLine();
                reader.close();
            }

            List<String> attributes = getAttributes(firstLine);
            List<String> potentialIdentifiers = getPotentialIdentifiers(attributes, suggestedDelimiters);

            if (!potentialIdentifiers.isEmpty()) {
                suggestions.put(CsvConfiguration.PROP_UNIQUE_ATTRIBUTE,
                        SuggestedValuesBuilder.buildOpen(potentialIdentifiers.toArray()));
                suggestions.put(CsvConfiguration.PROP_NAME_ATTRIBUTE,
                        SuggestedValuesBuilder.buildOpen(potentialIdentifiers.toArray()));
            }

            if (!attributes.isEmpty()) {
                suggestions.put(CsvConfiguration.PROP_PASSWORD_ATTRIBUTE,
                        SuggestedValuesBuilder.buildOpen(attributes.toArray()));
            }

        } catch (IOException e) {
            LOG.error("Couldn't create reader for file: {1} ({2})", configuration.getFilePath().getPath(), e.getMessage());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        return suggestions;
    }

    /**
     * Tries to select attributes that are good candidates for unique identifier and (unique) name. Analyzes first
     * {@link #MAX_PROCESSED_ROW} rows and checks for presence and uniqueness of each attribute.
     *
     * Note that this depends on the quality of suggested delimiters.
     *
     * If we don't know what to suggest, we return all attributes as potential candidates.
     * This is better than returning empty list and not suggesting anything at all.
     */
    private List<String> getPotentialIdentifiers(List<String> attributes, Map<String, SuggestedValues> suggestedDelimiters) {
        var parametersForDiscovery = determineParametersForDiscovery(suggestedDelimiters);
        if (parametersForDiscovery == null) {
            return attributes; // reason is already logged
        }

        LOG.ok("Trying to detect potential identifiers using {0}; known attributes: {1}", parametersForDiscovery, attributes);

        try {
            var potentialIdentifiers = new LinkedHashSet<>(attributes); // we want to preserve order of columns
            var valuesPerAttribute = new HashMap<String, Set<String>>(); // used to determine uniqueness of values per attribute
            var format = Util.createCsvFormatForDiscovery(parametersForDiscovery);
            int rowCount = 0;
            try (Reader reader = Util.createReader(configuration)) {
                for (CSVRecord record : format.parse(reader)) {
                    // TODO check and skip empty records
                    if (++rowCount >= MAX_PROCESSED_ROW) {
                        break;
                    }
                    for (Iterator<String> identifierIterator = potentialIdentifiers.iterator(); identifierIterator.hasNext(); ) {
                        String potentialIdentifier = identifierIterator.next();
                        String rawValue = record.get(potentialIdentifier);
                        String value = rawValue != null && rawValue.isBlank() ? null : rawValue;
                        boolean eligible;
                        if (value != null) {
                            if (!valuesPerAttribute
                                    .computeIfAbsent(potentialIdentifier, k -> new HashSet<>())
                                    .add(value)) {
                                LOG.ok("Value ''{0}'' for attribute {1} in row {2} is not unique, it cannot be an identifier",
                                        value, potentialIdentifier, rowCount);
                                eligible = false;
                            } else {
                                eligible = true;
                            }
                        } else {
                            LOG.ok("Empty value for attribute {0} in row {1}, it cannot be an identifier",
                                    potentialIdentifier, rowCount);
                            eligible = false;
                        }
                        if (!eligible) {
                            identifierIterator.remove();
                            valuesPerAttribute.remove(potentialIdentifier);
                        }
                    }
                }
            }
            LOG.ok("Detected potential identifiers: {0}", potentialIdentifiers);
            return new ArrayList<>(potentialIdentifiers);
        } catch (Exception e) {
            LOG.error(e, "Couldn't detect potential identifiers");
            return attributes;
        }
    }

    /**
     * Tries to determine parameters for potential identifiers discovery. If not successful, logs the reason and returns `null`.
     *
     * Preliminary version. We could e.g. try to determine things using multiple suggested delimiters, if there are more than one.
     */
    private static ParametersForDiscovery determineParametersForDiscovery(Map<String, SuggestedValues> suggestedDelimiters) {
        var fieldDelimiters = suggestedDelimiters.get(PROP_FIELD_DELIMITER);
        if (fieldDelimiters == null || fieldDelimiters.getValues().isEmpty()) {
            LOG.info("Couldn't find field delimiter, skipping potential identifiers detection");
            return null;
        }
        if (fieldDelimiters.getValues().size() > 1) {
            LOG.info("Multiple field delimiters were suggested, skipping potential identifiers detection");
            return null;
        }
        char fieldDelimiter = (char) fieldDelimiters.getValues().iterator().next();

        var commentMarkers = suggestedDelimiters.get(PROP_COMMENT_MARKER);
        Character commentMarker;
        if (commentMarkers != null && commentMarkers.getValues().size() == 1) {
            commentMarker = (char) commentMarkers.getValues().iterator().next();
        } else {
            commentMarker = null; // TODO resolve somehow
        }
        var quotes = suggestedDelimiters.get(PROP_QUOTE);
        Character quote;
        if (quotes != null && quotes.getValues().size() == 1) {
            quote = (char) quotes.getValues().iterator().next();
        } else {
            quote = null; // TODO resolve somehow
        }

        return new ParametersForDiscovery(fieldDelimiter, commentMarker, quote);
    }

    private List<String> getAttributes(String firstLine) {
        List<String> attributes = new ArrayList<>();
        int startIndex = 0;
        Integer endIndex = null;
        for(int i = 0; i < firstLine.length(); i++) {
            char ch = firstLine.charAt(i);
            if (isSymbol(ch) && ch != '_' && ch != '-') {
                endIndex = i;
            }

            if (endIndex != null) {

                String attribute = firstLine.substring(startIndex, endIndex);
                if (StringUtil.isNotEmpty(attribute)) {
                    attributes.add(attribute);
                }

                startIndex = endIndex + 1;
                endIndex = null;
            }
        }

        String attribute = firstLine.substring(startIndex);
        if (StringUtil.isNotEmpty(attribute)) {
            attributes.add(attribute);
        }

//        while (firstLine.toLowerCase().contains(attributeName)) {
//            int startIndex = firstLine.toLowerCase().indexOf(attributeName);
//            int endIndex = startIndex + attributeName.length();
//            int i;
//            for (i = startIndex; i >= 0; i--) {
//                char ch = firstLine.charAt(i);
//                if (isSymbol(ch) && ch != '_' && ch != '-') {
//                    break;
//                }
//            }
//            startIndex = i + 1;
//
//            for (i = endIndex; i < firstLine.length(); i++) {
//                char ch = firstLine.charAt(i);
//                if (isSymbol(ch) && ch != '_' && ch != '-') {
//                    break;
//                }
//            }
//            endIndex = i;
//            attributes.add(firstLine.substring(startIndex, endIndex));
//            firstLine = firstLine.substring(endIndex);
//        }
        return attributes;
    }

    private boolean isFirstInRow(int i, List<Character> characters) {
        if (i == 0) {
            return true;
        }
        return isNewLineChar(characters.get(--i));
    }

    private boolean isNewLineChar(char ch) {
        return ch == '\n' || ch == '\r';
    }

    private boolean isSymbol(char ch) {
        return !Character.isLetterOrDigit(ch) && (ch == '\t' || ch >= ' ');
    }

    private void increment(Map<Character, SymbolWrapper> map, char symbol, boolean isFirstInRow) {
        this.increment(map, symbol, isFirstInRow, 1);
    }

    private void increment(Map<Character, SymbolWrapper> map, char symbol, boolean isFirstInRow, int incrementSize) {
        SymbolWrapper wrapper = map.get(symbol);
        if (wrapper == null) {
            wrapper = new SymbolWrapper(isFirstInRow, 0);
        }
        wrapper.incrementCount(incrementSize);
        map.put(symbol, wrapper);
    }

    private void increment(Map<Character, Integer> map, Character symbol, int increment) {
        Integer count = map.get(symbol);
        if (count == null) {
            count = 0;
        }
        map.put(symbol, count + increment);
    }

    private static class SymbolWrapper {

        private final boolean isFirstInRow;
        private int countInRow;

        SymbolWrapper(boolean isFirstInRow, int countInRow) {
            this.isFirstInRow = isFirstInRow;
            this.countInRow = countInRow;
        }

        private void incrementCount(int increment) {
            countInRow += increment;
        }

        public boolean isFirstInRow() {
            return isFirstInRow;
        }

        public int getCountInRow() {
            return countInRow;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }
}
