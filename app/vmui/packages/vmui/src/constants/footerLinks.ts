import { CodeIcon, IssueIcon, WikiIcon } from "../components/Main/Icons";

const issueLink = {
  href: "https://github.com/VictoriaMetrics/VictoriaLogs/issues/new/choose",
  Icon: IssueIcon,
  title: "Create an issue",
};

export const footerLinksToLogs = [
  {
    href: "https://docs.victoriametrics.com/victorialogs/logsql/",
    Icon: CodeIcon,
    title: "LogsQL",
  },
  {
    href: "https://docs.victoriametrics.com/victorialogs/",
    Icon: WikiIcon,
    title: "Documentation",
  },
  issueLink
];
