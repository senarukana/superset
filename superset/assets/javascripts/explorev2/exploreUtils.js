/* eslint camelcase: 0 */
export function getExploreUrl(form_data, endpoint = 'base') {
  let params = '?form_data=' + encodeURIComponent(JSON.stringify(form_data));
  switch (endpoint) {
    case 'base':
      return `/superset/explore/${params}`;
    case 'json':
      return `/superset/explore_json/${params}`;
    case 'csv':
      return `/superset/explore_json/${params}&csv=true`;
    case 'standalone':
      return `/superset/explore/${params}&standalone=true`;
    case 'query':
      return `/superset/explore_json/${params}&query=true`;
    default:
      return `/superset/explore/${params}`;
  }
}
