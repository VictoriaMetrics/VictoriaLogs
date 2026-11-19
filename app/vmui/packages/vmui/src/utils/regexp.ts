export const escapeRegexp = (s: string) => {
  // Escapes the same chars as Go's regexp.QuoteMeta, since VictoriaLogs uses RE2 syntax.
  return s.replace(/[\\.+*?()|[\]{}^$]/g, "\\$&");
};

export const escapeForLogsQLString = (s: string) => {
  return JSON.stringify(s).slice(1,-1);
};

export const hasUnclosedQuotes = (str: string) => {
  const matches = str.match(/["`']/g);
  return matches ? matches.length % 2 !== 0 : false;
};
