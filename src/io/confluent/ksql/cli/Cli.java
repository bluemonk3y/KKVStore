package io.confluent.ksql.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.*;
import io.confluent.ksql.rest.server.computation.CommandId;
import io.confluent.ksql.util.Version;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultExpander;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Cli implements Closeable, AutoCloseable {
    private static final String DEFAULT_PRIMARY_PROMPT = "ksql> ";
    private static final String DEFAULT_SECONDARY_PROMPT = "      ";
    private static final Pattern QUOTED_PROMPT_PATTERN = Pattern.compile("\'(\'\'|[^\'])*\'");
    private final ExecutorService queryStreamExecutorService;
    private final LinkedHashMap<String, CliSpecificCommand> cliSpecificCommands;
    private final ObjectMapper objectMapper;
    private final Long streamedQueryRowLimit;
    private final Long streamedQueryTimeoutMs;
    protected final KsqlRestClient restClient;
    protected final Terminal terminal;
    protected final LineReader lineReader;
    private final DefaultHistory history;
    private String primaryPrompt;
    private Cli.OutputFormat outputFormat;

    public Cli(KsqlRestClient restClient, Long streamedQueryRowLimit, Long streamedQueryTimeoutMs, Cli.OutputFormat outputFormat) throws IOException {
        Objects.requireNonNull(restClient, "Must provide the CLI with a REST client");
        Objects.requireNonNull(outputFormat, "Must provide the CLI with a beginning output format");
        System.out.println("BOOM NEIL-CLI 22222");


        this.streamedQueryRowLimit = streamedQueryRowLimit;
        this.streamedQueryTimeoutMs = streamedQueryTimeoutMs;
        this.restClient = restClient;
        this.terminal = TerminalBuilder.builder().system(true).build();
        this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
        DefaultParser parser = new DefaultParser();

        parser.setEofOnEscapedNewLine(true);
        parser.setQuoteChars(new char[0]);
        parser.setEscapeChars(new char[]{'\\'});
        Cli.NoOpExpander expander = new Cli.NoOpExpander();
        this.lineReader = LineReaderBuilder.builder().appName("KSQL").expander(expander).parser(parser).terminal(this.terminal).build();
        this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_DUPS);
        this.lineReader.setOpt(LineReader.Option.HISTORY_IGNORE_SPACE);

        this.lineReader.setVariable(LineReader.HISTORY_FILE, Paths.get("cli-history"));
        this.lineReader.unsetOpt(LineReader.Option.HISTORY_INCREMENTAL);
        this.history = new DefaultHistory(this.lineReader);

        this.lineReader.setVariable("secondary-prompt-pattern", "      ");
        this.primaryPrompt = "ksql> ";
        this.outputFormat = outputFormat;
        this.queryStreamExecutorService = Executors.newSingleThreadExecutor();
        this.cliSpecificCommands = new LinkedHashMap();
        this.registerDefaultCliSpecificCommands();
        this.objectMapper = (new ObjectMapper()).disable(new JsonGenerator.Feature[]{JsonGenerator.Feature.AUTO_CLOSE_TARGET});
        (new SchemaMapper()).registerToObjectMapper(this.objectMapper);
    }

    public void runInteractively() throws IOException {
        this.displayWelcomeMessage();

        while(true) {
            try {
                this.handleLine(this.readLine());
            } catch (EndOfFileException var2) {
                return;
            } catch (Exception var3) {
                if(var3.getMessage() != null) {
                    this.terminal.writer().println(var3.getMessage());
                } else {
                    this.terminal.writer().println(var3.getClass().getName());
                }
            }

            this.terminal.flush();
        }
    }

    private void displayWelcomeMessage() {
        String serverVersion;
        try {
            serverVersion = ((ServerInfo)this.restClient.makeRootRequest().getResponse()).getVersion();
        } catch (Exception var9) {
            serverVersion = "<unknown>";
        }

        String cliVersion = Version.getVersion();
        byte logoWidth = 33;
        String copyrightMessage = "Copyright 2017 Confluent Inc.";
        String helpReminderMessage = "Having trouble? Need quick refresher? Wanna RTFM but perturbed by our lack of man pages? Type \'help\' (case-insensitive) for a rundown of how things work!";
        if(this.terminal.getWidth() >= logoWidth) {
            int paddedLogoWidth = Math.min(this.terminal.getWidth(), helpReminderMessage.length());
            int paddingWidth = (paddedLogoWidth - logoWidth) / 2;
            String leftPadding = (new String(new byte[paddingWidth])).replaceAll(".", " ");
            this.terminal.writer().printf("%s=================================%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=   _  __ _____  ____  _        =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=  | |/ // ____|/ __ \\| |       =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=  | \' /| (___ | |  | | |       =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=  |  <  \\___ \\| |  | | |       =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=  | . \\ ____) | |__| | |____   =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=  |_|\\_\\_____/ \\___\\_\\______|  =%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s=================================%n", new Object[]{leftPadding});
            this.terminal.writer().printf("%s  %s%n", new Object[]{copyrightMessage, leftPadding});
        } else {
            this.terminal.writer().printf("KSQL, %s%n", new Object[]{copyrightMessage});
        }

        this.terminal.writer().println();
        this.terminal.writer().printf("CLI v%s, Server v%s located at %s%n", new Object[]{cliVersion, serverVersion, this.restClient.getServerAddress()});
        this.terminal.writer().println();
        this.terminal.writer().println(helpReminderMessage);
        this.terminal.writer().println();
        this.terminal.flush();
    }

    public void runNonInteractively(String input) throws Exception {
        Iterator var2 = this.getLogicalLines(input).iterator();

        while(var2.hasNext()) {
            String logicalLine = (String)var2.next();

            try {
                this.handleLine(logicalLine);
            } catch (EndOfFileException var5) {
                return;
            }
        }

    }

    private List<String> getLogicalLines(String input) {
        ArrayList result = new ArrayList();
        StringBuilder logicalLine = new StringBuilder();
        String[] var4 = input.split("\n");
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            String physicalLine = var4[var6];
            if(!physicalLine.trim().isEmpty()) {
                if(physicalLine.endsWith("\\")) {
                    logicalLine.append(physicalLine.substring(0, physicalLine.length() - 1));
                } else {
                    result.add(logicalLine.append(physicalLine).toString().trim());
                    logicalLine = new StringBuilder();
                }
            }
        }

        return result;
    }

    public void close() throws IOException {
        this.queryStreamExecutorService.shutdownNow();
        this.restClient.close();
        this.terminal.close();
    }

    protected void registerCliSpecificCommand(Cli.CliSpecificCommand cliSpecificCommand) {
        this.cliSpecificCommands.put(cliSpecificCommand.getName(), cliSpecificCommand);
    }

    private void registerDefaultCliSpecificCommands() {
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "help";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\thelp: Show this message");
            }

            public void execute(String line) {
                Cli.this.terminal.writer().println("CLI-specific commands (EACH MUST BE ON THEIR OWN LINE):");
                Cli.this.terminal.writer().println();
                Iterator var2 = Cli.this.cliSpecificCommands.values().iterator();

                while(var2.hasNext()) {
                    Cli.CliSpecificCommand cliSpecificCommand = (Cli.CliSpecificCommand)var2.next();
                    cliSpecificCommand.printHelp();
                    Cli.this.terminal.writer().println();
                }

                Cli.this.terminal.writer().println();
                Cli.this.terminal.writer().println("default behavior:");
                Cli.this.terminal.writer().println();
                Cli.this.terminal.writer().println("    Lines are read one at a time and are sent to the server asKsql unless one of the following is true:");
                Cli.this.terminal.writer().println();
                Cli.this.terminal.writer().println("    1. The line is empty or entirely whitespace. In this case, no request is made to the server.");
                Cli.this.terminal.writer().println();
                Cli.this.terminal.writer().println("    2. The line ends with \'\\\'. In this case, lines are continuously read and stripped of their trailing newline and \'\\\' until one is encountered that does not end with \'\\\'; then, the concatenation of all lines read during this time is sent to the server as Ksql.");
                Cli.this.terminal.writer().println();
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "clear";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\tclear: Clear the current terminal");
            }

            public void execute(String commandStrippedLine) throws IOException {
                Cli.this.terminal.puts(InfoCmp.Capability.clear_screen, new Object[0]);
                Cli.this.terminal.flush();
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "status";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\tstatus:      Get status information on all distributed statements");
                Cli.this.terminal.writer().println("\tstatus <id>: Get detailed status information on the command with an ID of <id>");
                Cli.this.terminal.writer().println("\t             example: \"status stream/MY_AWESOME_KSQL_STREAM\"");
            }

            public void execute(String commandStrippedLine) throws IOException {
                if(commandStrippedLine.trim().isEmpty()) {
                    RestResponse statementId = Cli.this.restClient.makeStatusRequest();
                    if(statementId.isSuccessful()) {
                        Cli.this.printCommandStatuses((CommandStatuses)statementId.getResponse());
                    } else {
                        Cli.this.printErrorMessage(statementId.getErrorMessage());
                    }
                } else {
                    String statementId1 = commandStrippedLine.trim();
                    RestResponse response = Cli.this.restClient.makeStatusRequest(statementId1);
                    if(response.isSuccessful()) {
                        Cli.this.printCommandStatus((CommandStatus)response.getResponse());
                    } else {
                        Cli.this.printErrorMessage(response.getErrorMessage());
                    }
                }

            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "prompt";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\tprompt <prompt>: Set the primary prompt to <prompt>");
                Cli.this.terminal.writer().println("\t                 example: \"prompt my_awesome_prompt>\", or \"prompt \'my \'\'awesome\'\' prompt> \'\"");
            }

            public void execute(String commandStrippedLine) {
                Cli.this.primaryPrompt = Cli.this.parsePromptString(commandStrippedLine);
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "prompt2";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\tprompt2 <prompt>: Set the secondary prompt to <prompt>");
                Cli.this.terminal.writer().println("\t                  example: \"prompt2 my_awesome_prompt\", or \"prompt2 \'my \'\'awesome\'\' prompt> \'\"");
            }

            public void execute(String commandStrippedLine) throws IOException {
                Cli.this.lineReader.setVariable("secondary-prompt-pattern", Cli.this.parsePromptString(commandStrippedLine));
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            private final String outputFormats = String.format("\'%s\'", new Object[]{String.join("\', \'", (Iterable)Arrays.stream(Cli.OutputFormat.values()).map(Object::toString).collect(Collectors.toList()))});

            public String getName() {
                return "output";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\toutput:          View the current output format");
                Cli.this.terminal.writer().printf("\toutput <format>: Set the output format to <format> (valid formats: %s)%n", new Object[]{this.outputFormats});
                Cli.this.terminal.writer().println("\t                 example: \"output JSON\"");
            }

            public void execute(String commandStrippedLine) throws IOException {
                String newFormat = commandStrippedLine.trim().toUpperCase();
                if(newFormat.isEmpty()) {
                    Cli.this.terminal.writer().printf("Current output format: %s%n", new Object[]{Cli.this.outputFormat.name()});
                } else {
                    try {
                        Cli.this.outputFormat = Cli.OutputFormat.valueOf(newFormat);
                        Cli.this.terminal.writer().printf("Output format set to %s%n", new Object[]{Cli.this.outputFormat.name()});
                    } catch (IllegalArgumentException var4) {
                        Cli.this.terminal.writer().printf("Invalid output format: \'%s\' (valid formats: %s)%n", new Object[]{newFormat, this.outputFormats});
                    }
                }

            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "history";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\thistory: Show previous lines entered during the current CLI session");
            }

            public void execute(String commandStrippedLine) throws IOException {
                ListIterator var2 = Cli.this.lineReader.getHistory().iterator();

                while(var2.hasNext()) {
                    History.Entry historyEntry = (History.Entry)var2.next();
                    Cli.this.terminal.writer().printf("%4d: %s%n", new Object[]{Integer.valueOf(historyEntry.index()), historyEntry.line()});
                }

                Cli.this.terminal.flush();
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "version";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\tversion: Get the current KSQL version");
            }

            public void execute(String commandStrippedLine) throws IOException {
                ServerInfo serverInfo = (ServerInfo)Cli.this.restClient.makeRootRequest().getResponse();
                Cli.this.terminal.writer().printf("Version: %s%n", new Object[]{serverInfo.getVersion()});
                Cli.this.terminal.flush();
            }
        });
        this.registerCliSpecificCommand(new Cli.CliSpecificCommand() {
            public String getName() {
                return "exit";
            }

            public void printHelp() {
                Cli.this.terminal.writer().println("\texit: Exit the CLI; EOF (i.e., ^D) works as well");
            }

            public void execute(String commandStrippedLine) throws IOException {
                throw new EndOfFileException();
            }
        });
    }

    private String parsePromptString(String commandStrippedLine) {
        if(commandStrippedLine.trim().isEmpty()) {
            throw new RuntimeException("Prompt command must be followed by a new prompt to use");
        } else {
            String trimmedLine = commandStrippedLine.trim().replace("%", "%%");
            if(trimmedLine.contains("\'")) {
                Matcher quotedPromptMatcher = QUOTED_PROMPT_PATTERN.matcher(trimmedLine);
                if(quotedPromptMatcher.matches()) {
                    return trimmedLine.substring(1, trimmedLine.length() - 1).replace("\'\'", "\'");
                } else {
                    throw new RuntimeException("Failed to parse prompt string. All non-enclosing single quotes must be doubled.");
                }
            } else {
                return trimmedLine;
            }
        }
    }

    private void handleLine(String line) throws Exception {
        history.add(line);
        history.save();
        String trimmedLine = ((String)Optional.ofNullable(line).orElse("")).trim();
        if(!trimmedLine.isEmpty()) {
            String[] commandArgs = trimmedLine.split("\\s+", 2);
            Cli.CliSpecificCommand cliSpecificCommand = (Cli.CliSpecificCommand)this.cliSpecificCommands.get(commandArgs[0].toLowerCase());
            if(cliSpecificCommand != null) {
                cliSpecificCommand.execute(commandArgs.length > 1?commandArgs[1]:"");
            } else {
                this.handleStatements(line);
            }

        }

    }

    private String readLine() throws IOException {
        while(true) {
            try {
                String exception = this.lineReader.readLine(this.primaryPrompt);
                if(exception == null) {
                    throw new EndOfFileException();
                }

                return exception.trim();
            } catch (UserInterruptException var2) {
                this.terminal.writer().println("^C");
                this.terminal.flush();
            }
        }
    }

    private void handleStatements(String line) throws IOException, InterruptedException, ExecutionException {
        StringBuilder consecutiveStatements = new StringBuilder();
        Iterator var3 = (new KsqlParser()).getStatements(line).iterator();

        while(true) {
            while(var3.hasNext()) {
                SqlBaseParser.SingleStatementContext statementContext = (SqlBaseParser.SingleStatementContext)var3.next();
                String statementText = KsqlEngine.getStatementString(statementContext);
                if(!(statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) && !(statementContext.statement() instanceof SqlBaseParser.PrintTopicContext)) {
                    consecutiveStatements.append(statementText);
                } else {
                    if(consecutiveStatements.length() != 0) {
                        this.printKsqlResponse(this.restClient.makeKsqlRequest(consecutiveStatements.toString()));
                        consecutiveStatements = new StringBuilder();
                    }

                    if(statementContext.statement() instanceof SqlBaseParser.QuerystatementContext) {
                        this.handleStreamedQuery(statementText);
                    } else {
                        this.handlePrintedTopic(statementText);
                    }
                }
            }

            if(consecutiveStatements.length() != 0) {
                this.printKsqlResponse(this.restClient.makeKsqlRequest(consecutiveStatements.toString()));
            }

            return;
        }
    }

    private void handleStreamedQuery(String query) throws IOException, InterruptedException, ExecutionException {
        RestResponse queryResponse = this.restClient.makeQueryRequest(query);
        if(queryResponse.isSuccessful()) {
            try {
                final KsqlRestClient.QueryStream queryStream = (KsqlRestClient.QueryStream)queryResponse.getResponse();
                Throwable var4 = null;

                try {
                    final Future queryStreamFuture = this.queryStreamExecutorService.submit(new Runnable() {
                        public void run() {
                            for(long rowsRead = 0L; Cli.this.keepReading(rowsRead) && queryStream.hasNext(); ++rowsRead) {
                                try {
                                    Cli.this.printStreamedRow(queryStream.next());
                                } catch (IOException var4) {
                                    throw new RuntimeException(var4);
                                }
                            }

                        }
                    });
                    this.terminal.handle(Terminal.Signal.INT, new Terminal.SignalHandler() {
                        public void handle(Terminal.Signal signal) {
                            Cli.this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
                            queryStreamFuture.cancel(true);
                        }
                    });

                    try {
                        if(this.streamedQueryTimeoutMs == null) {
                            queryStreamFuture.get();
                            Thread.sleep(1000L);
                        } else {
                            try {
                                queryStreamFuture.get(this.streamedQueryTimeoutMs.longValue(), TimeUnit.MILLISECONDS);
                            } catch (TimeoutException var25) {
                                queryStreamFuture.cancel(true);
                            }
                        }
                    } catch (CancellationException var26) {
                        ;
                    }
                } catch (Throwable var27) {
                    var4 = var27;
                    throw var27;
                } finally {
                    if(queryStream != null) {
                        if(var4 != null) {
                            try {
                                queryStream.close();
                            } catch (Throwable var24) {
                                var4.addSuppressed(var24);
                            }
                        } else {
                            queryStream.close();
                        }
                    }

                }
            } finally {
                this.terminal.writer().println("Query terminated");
                this.terminal.flush();
            }
        } else {
            this.printErrorMessage(queryResponse.getErrorMessage());
        }

    }

    private boolean keepReading(long rowsRead) {
        return this.streamedQueryRowLimit == null || rowsRead < this.streamedQueryRowLimit.longValue();
    }

    private void handlePrintedTopic(String printTopic) throws InterruptedException, ExecutionException, IOException {
        RestResponse topicResponse = this.restClient.makePrintTopicRequest(printTopic);
        if(topicResponse.isSuccessful()) {
            final Scanner topicStreamScanner = new Scanner((InputStream)topicResponse.getResponse());
            Throwable var4 = null;

            try {
                final Future topicPrintFuture = this.queryStreamExecutorService.submit(new Runnable() {
                    public void run() {
                        while(topicStreamScanner.hasNextLine()) {
                            String line = topicStreamScanner.nextLine();
                            if(!line.isEmpty()) {
                                Cli.this.terminal.writer().println(line);
                                Cli.this.terminal.flush();
                            }
                        }

                    }
                });
                this.terminal.handle(Terminal.Signal.INT, new Terminal.SignalHandler() {
                    public void handle(Terminal.Signal signal) {
                        Cli.this.terminal.handle(Terminal.Signal.INT, Terminal.SignalHandler.SIG_IGN);
                        topicPrintFuture.cancel(true);
                    }
                });

                try {
                    topicPrintFuture.get();
                } catch (CancellationException var15) {
                    this.terminal.writer().println("Topic printing ceased");
                    this.terminal.flush();
                }

                ((InputStream)topicResponse.getResponse()).close();
            } catch (Throwable var16) {
                var4 = var16;
                throw var16;
            } finally {
                if(topicStreamScanner != null) {
                    if(var4 != null) {
                        try {
                            topicStreamScanner.close();
                        } catch (Throwable var14) {
                            var4.addSuppressed(var14);
                        }
                    } else {
                        topicStreamScanner.close();
                    }
                }

            }
        } else {
            this.terminal.writer().println(topicResponse.getErrorMessage().getMessage());
            this.terminal.flush();
        }

    }

    private void printKsqlResponse(RestResponse<KsqlEntityList> response) throws IOException {
        if(response.isSuccessful()) {
            this.printKsqlEntityList((List)response.getResponse());
        } else {
            this.printErrorMessage(response.getErrorMessage());
        }

    }

    private void printStreamedRow(StreamedRow row) throws IOException {
        if(row.getErrorMessage() != null) {
            this.printErrorMessage(row.getErrorMessage());
        } else {
//            switch(null.$SwitchMap$io$confluent$ksql$cli$Cli$OutputFormat[this.outputFormat.ordinal()]) {
//                case 1:
                    this.printAsJson(row.getRow().columns);
//                    break;
//                case 2:
//                    this.printAsTable(row.getRow());
//                    break;
//                default:
//                    throw new RuntimeException(String.format("Unexpected output format: \'%s\'", new Object[]{this.outputFormat.name()}));
//            }
        }

    }

    private void printCommandStatus(CommandStatus status) throws IOException {
//        switch(null.$SwitchMap$io$confluent$ksql$cli$Cli$OutputFormat[this.outputFormat.ordinal()]) {
//            case 1:
                this.printAsJson(status);
//                break;
//            case 2:
//                this.printAsTable(status);
//                break;
//            default:
//                throw new RuntimeException(String.format("Unexpected output format: \'%s\'", new Object[]{this.outputFormat.name()}));
//        }

    }

    private void printCommandStatuses(CommandStatuses statuses) throws IOException {
//        switch(null.$SwitchMap$io$confluent$ksql$cli$Cli$OutputFormat[this.outputFormat.ordinal()]) {
//            case 1:
                this.printAsJson(statuses);
//                break;
//            case 2:
//                this.printAsTable(statuses);
//                break;
//            default:
//                throw new RuntimeException(String.format("Unexpected output format: \'%s\'", new Object[]{this.outputFormat.name()}));
//        }

    }

    private void printKsqlEntityList(List<KsqlEntity> entityList) throws IOException {
//        switch(null.$SwitchMap$io$confluent$ksql$cli$Cli$OutputFormat[this.outputFormat.ordinal()]) {
//            case 1:
                this.printAsJson(entityList);
//                return;
//            case 2:
//                Iterator var2 = entityList.iterator();
//
//                while(var2.hasNext()) {
//                    KsqlEntity ksqlEntity = (KsqlEntity)var2.next();
//                    this.terminal.writer().println();
//                    this.printAsTable(ksqlEntity);
//                }
//
//                return;
//            default:
//                throw new RuntimeException(String.format("Unexpected output format: \'%s\'", new Object[]{this.outputFormat.name()}));
//        }
    }

    private void printErrorMessage(ErrorMessage errorMessage) {
        this.terminal.writer().println(errorMessage.getMessage());
    }

    private void printAsJson(Object o) throws IOException {
        this.objectMapper.writerWithDefaultPrettyPrinter().writeValue(this.terminal.writer(), o);
        this.terminal.writer().println();
        this.terminal.flush();
    }

    private void printAsTable(KsqlEntity ksqlEntity) {
        List columnHeaders;
        List rowValues;
        if(ksqlEntity instanceof CommandStatusEntity) {
            CommandStatusEntity topicInfos = (CommandStatusEntity)ksqlEntity;
            columnHeaders = Arrays.asList(new String[]{"Command ID", "Status", "Message"});
            CommandId commandId = topicInfos.getCommandId();
            CommandStatus commandStatus = topicInfos.getCommandStatus();
            rowValues = Collections.singletonList(Arrays.asList(new String[]{commandId.toString(), commandStatus.getStatus().name(), commandStatus.getMessage().split("\n", 2)[0]}));
        } else {
            if(ksqlEntity instanceof ErrorMessageEntity) {
                ErrorMessage topicInfos4 = ((ErrorMessageEntity)ksqlEntity).getErrorMessage();
                this.printErrorMessage(topicInfos4);
                return;
            }

            if(ksqlEntity instanceof PropertiesList) {
                Map topicInfos1 = ((PropertiesList)ksqlEntity).getProperties();
                columnHeaders = Arrays.asList(new String[]{"Property", "Value"});
//                rowValues = (List)topicInfos1.entrySet().stream().map((propertyEntry) -> {
//                    return Arrays.asList(new String[]{(String)propertyEntry.getKey(), Objects.toString(propertyEntry.getValue())});
//                }).collect(Collectors.toList());
            } else {
//                List topicInfos2;
//                if(ksqlEntity instanceof Queries) {
//                    topicInfos2 = ((Queries)ksqlEntity).getQueries();
//                    columnHeaders = Arrays.asList(new String[]{"ID", "Kafka Topic", "Query String"});
//                    rowValues = (List)topicInfos2.stream().map((runningQuery) -> {
//                        return Arrays.asList(new String[]{Long.toString(runningQuery.getId()), runningQuery.getKafkaTopic(), runningQuery.getQueryString()});
//                    }).collect(Collectors.toList());
//                } else if(ksqlEntity instanceof SetProperty) {
//                    SetProperty topicInfos3 = (SetProperty)ksqlEntity;
//                    columnHeaders = Arrays.asList(new String[]{"Property", "Prior Value", "New Value"});
//                    rowValues = Collections.singletonList(Arrays.asList(new String[]{topicInfos3.getProperty(), Objects.toString(topicInfos3.getOldValue()), Objects.toString(topicInfos3.getNewValue())}));
//                } else if(ksqlEntity instanceof SourceDescription) {
//                    topicInfos2 = ((SourceDescription)ksqlEntity).getSchema().fields();
//                    columnHeaders = Arrays.asList(new String[]{"Field", "Type"});
//                    rowValues = (List)topicInfos2.stream().map((field) -> {
//                        return Arrays.asList(new String[]{field.name(), field.schema().type().toString()});
//                    }).collect(Collectors.toList());
//                } else if(ksqlEntity instanceof StreamsList) {
//                    topicInfos2 = ((StreamsList)ksqlEntity).getStreams();
//                    columnHeaders = Arrays.asList(new String[]{"Stream Name", "Ksql Topic"});
//                    rowValues = (List)topicInfos2.stream().map((streamInfo) -> {
//                        return Arrays.asList(new String[]{streamInfo.getName(), streamInfo.getTopic()});
//                    }).collect(Collectors.toList());
//                } else if(ksqlEntity instanceof TablesList) {
//                    topicInfos2 = ((TablesList)ksqlEntity).getTables();
//                    columnHeaders = Arrays.asList(new String[]{"Table Name", "Ksql Topic", "Statestore", "Windowed"});
//                    rowValues = (List)topicInfos2.stream().map((tableInfo) -> {
//                        return Arrays.asList(new String[]{tableInfo.getName(), tableInfo.getTopic(), tableInfo.getStateStoreName(), Boolean.toString(tableInfo.getIsWindowed())});
//                    }).collect(Collectors.toList());
//                } else {
//                    if(!(ksqlEntity instanceof TopicsList)) {
//                        throw new RuntimeException(String.format("Unexpected KsqlEntity class: \'%s\'", new Object[]{ksqlEntity.getClass().getCanonicalName()}));
//                    }
//
//                    topicInfos2 = ((TopicsList)ksqlEntity).getTopics();
//                    columnHeaders = Arrays.asList(new String[]{"Topic Name", "Kafka Topic", "Format"});
//                    rowValues = (List)topicInfos2.stream().map((topicInfo) -> {
//                        return Arrays.asList(new String[]{topicInfo.getName(), topicInfo.getKafkaTopic(), topicInfo.getFormat().name()});
//                    }).collect(Collectors.toList());
//                }
            }
        }

//        this.printTable(columnHeaders, rowValues);
    }

//    private void printAsTable(GenericRow row) {
//        this.terminal.writer().println(String.join(" | ", (Iterable)row.columns.stream().map(Objects::toString).collect(Collectors.toList())));
//        this.terminal.flush();
//    }

//    private void printAsTable(CommandStatuses statuses) {
//        List columnHeaders = Arrays.asList(new String[]{"Command ID", "Status"});
//        List rowValues = (List)statuses.entrySet().stream().map((statusEntry) -> {
//            return Arrays.asList(new String[]{((CommandId)statusEntry.getKey()).toString(), ((CommandStatus.Status)statusEntry.getValue()).name()});
//        }).collect(Collectors.toList());
//        this.printTable(columnHeaders, rowValues);
//    }

//    private void printAsTable(CommandStatus status) {
//        this.printTable(Arrays.asList(new String[]{"Status", "Message"}), Collections.singletonList(Arrays.asList(new String[]{status.getStatus().name(), status.getMessage().split("\n", 2)[0]})));
//    }

    private void printTable(List<String> columnHeaders, List<List<String>> rowValues) {
        if(columnHeaders.size() == 0) {
            throw new RuntimeException("Cannot print table without columns");
        } else {
            Integer[] columnLengths = new Integer[columnHeaders.size()];
            int separatorLength = -1;

            for(int rowFormatString = 0; rowFormatString < columnLengths.length; ++rowFormatString) {
                int columnLength = ((String)columnHeaders.get(rowFormatString)).length();

                List row1;
                for(Iterator row = rowValues.iterator(); row.hasNext(); columnLength = Math.max(columnLength, ((String)row1.get(rowFormatString)).length())) {
                    row1 = (List)row.next();
                }

                columnLengths[rowFormatString] = Integer.valueOf(columnLength);
                separatorLength += columnLength + 3;
            }

            String var9 = constructRowFormatString(columnLengths);
            this.terminal.writer().printf(var9, columnHeaders.toArray());
            this.terminal.writer().println((new String(new char[separatorLength])).replaceAll(".", "-"));
            Iterator var10 = rowValues.iterator();

            while(var10.hasNext()) {
                List var11 = (List)var10.next();
                this.terminal.writer().printf(var9, var11.toArray());
            }

            this.terminal.flush();
        }
    }

    private static String constructRowFormatString(Integer... lengths) {
        List columnFormatStrings = (List)Arrays.stream(lengths).map(Cli::constructSingleColumnFormatString).collect(Collectors.toList());
        return String.format(" %s %n", new Object[]{String.join(" | ", columnFormatStrings)});
    }

    private static String constructSingleColumnFormatString(Integer length) {
        return String.format("%%%ds", new Object[]{length});
    }

    private static class NoOpExpander extends DefaultExpander {
        private NoOpExpander() {
        }

        public String expandHistory(History history, String line) {
            return line;
        }
    }

    protected interface CliSpecificCommand {
        String getName();

        void printHelp();

        void execute(String var1) throws IOException;
    }

    public static enum OutputFormat {
        JSON,
        TABULAR;

        private OutputFormat() {
        }
    }
}
