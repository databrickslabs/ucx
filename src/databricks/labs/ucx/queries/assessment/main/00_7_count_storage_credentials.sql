/* --title 'Total Storage Credentials'
--height 3
--overrides '{
               "spec": {
                 "encodings": {
                   "value": {
                     "fieldName": "count_total_storage_credentials",
                     "rowNumber": 2,
                     "style": {
                       "rules": [
                         {
                           "condition": {
                             "operator": ">",
                             "operand": {
                               "type": "data-value",
                               "value": "200"
                             }
                           },
                           "color": "#E92828"
                         }
                       ]
                     },
                     "displayName": "count_total_storage_credentials"
                   }
                 }
               }
             }'
*/
SELECT
  COUNT(*) AS count_total_storage_credentials
FROM inventory.external_locations