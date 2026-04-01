import { CodeIcon, IssueIcon, WikiIcon } from "../components/Main/Icons";
import { LOGS_DOCS_URL } from "./logs";

const issueLink = {
  href: "https://github.com/VictoriaMetrics/VictoriaLogs/issues/new/choose",
  Icon: IssueIcon,
  title: "Create an issue",
};

export const footerLinksToLogs = [
  {
    href: `${LOGS_DOCS_URL}/logsql/`,
    Icon: CodeIcon,
    title: "LogsQL",
  },
  {
    href: `${LOGS_DOCS_URL}`,
    Icon: WikiIcon,
    title: "Documentation",
  },
  issueLink
];
