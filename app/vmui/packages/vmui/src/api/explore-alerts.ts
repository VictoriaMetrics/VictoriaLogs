export const getGroupsUrl = (server: string): string => {
  return `${server}/select/vmalert/api/v1/rules?datasource_type=vlogs`;
};

export const getItemUrl = (
  server: string,
  groupId: string,
  id: string,
  mode: string,
): string => {
  return `${server}/select/vmalert/api/v1/${mode}?group_id=${groupId}&${mode}_id=${id}`;
};

export const getGroupUrl = (
  server: string,
  id: string,
): string => {
  return `${server}/select/vmalert/api/v1/group?group_id=${id}`;
};

export const getNotifiersUrl = (server: string): string => {
  return `${server}/select/vmalert/api/v1/notifiers`;
};
