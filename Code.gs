const cc = DataStudioApp.createCommunityConnector();
const url = 'https://api.monday.com/v2'
const apiVersion = '2024-10'

function getAuthType() {
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl("https://developer.monday.com/api-reference/docs")
    .build();
}

// Used to manually reset/disconnect the connector from the datastudio report
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

// Runs everytime the script runs to make sure that the credentials are valid
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  if (key != null) {
    var query = "query { boards (limit:1) {id name} }";
    var options = {
      "method" : "post",
      "contentType" : "application/json",
      "headers" : {
        "Authorization" : key,
        "API-Version": apiVersion
      },
      "payload" : JSON.stringify({
        "query" : query
      }),
      "muteHttpExceptions":true
    };
    var res = UrlFetchApp.fetch(url, options);
    var valid_res = res.getResponseCode() == 200;
    Logger.log("Request is valid")
  } else {
    var valid_res = false;
    Logger.log("Request is not valid")
  }
  
  return valid_res;
};

// Allows the user to input their credentials, and if they're valid, it saves them for the user (not developer), else it returns error
function setCredentials(request) {
  Logger.log("Setting new credentials")
  var key = request.key;
  var query = "query { boards (limit:1) {id name} }";
  var options = {
    "method" : "post",
    "contentType" : "application/json",
    "headers" : {
      "Authorization" : key,
      "API-Version": apiVersion
    },
    "payload" : JSON.stringify({
      "query" : query
    }),
    "muteHttpExceptions":true
  };
  
  var res = UrlFetchApp.fetch(url, options);

  if (res.getResponseCode() != 200) {
    return cc.newSetCredentialsResponse()
      .setIsValid(false)
      .build();
  } else {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('dscc.key', key);
  return cc.newSetCredentialsResponse()
    .setIsValid(true)
    .build();
  }
  
}

function getConfig() {
  const config = cc.getConfig();
  
  config.newTextInput()
  .setId('board_id')
  .setName("Please enter the board ID that you want to sync from Monday");

  config.newInfo()
    .setText('This integration uses Mondays new cursor based queries, and loops through records in batches of 100 at a time to manage the capacity constraints and API limitations imposed by the Monday.com API');

  Logger.log("Config: "+config.printJson());
  return config.build();
}

function queryBoardFields(board_id) {
  
  const key = PropertiesService.getUserProperties().getProperty('dscc.key');

  let query = `query { boards (ids: [${board_id}]) { columns { id title type description } } }`;
  let options = {
    "method" : "post",
    "contentType" : "application/json",
    "headers" : {
        "Authorization" : key,
        "API-Version": apiVersion
    },
    "payload" : JSON.stringify({
      "query" : query
    })
  };

  
  let col_res = UrlFetchApp.fetch(url,options);
  col_res = JSON.parse(col_res);

  return col_res;
}

function getFields(request) {
  const fields = cc.getFields();
  var board_id = request.configParams['board_id'];
  Logger.log("Getting fields for board_id: "+board_id)
  col_res = queryBoardFields(board_id);

  // Set the standard columns
  fields.newDimension()
      .setId('board_id')
      .setName('Board id')
      .setDescription('The id of the board')
      .setType(cc.FieldType.NUMBER);

  fields.newDimension()
      .setId('id')
      .setName('Item id')
      .setDescription('The id of the item')
      .setType(cc.FieldType.NUMBER);

  fields.newDimension()
      .setId('board_name')
      .setName('Board name')
      .setDescription('The name of the board')
      .setType(cc.FieldType.TEXT);

  fields.newDimension()
      .setId('board_state')
      .setName('Board state')
      .setDescription('The state of the board')
      .setType(cc.FieldType.TEXT);

  fields.newDimension()
      .setId('item_name')
      .setName('Item name')
      .setDescription('The name of the item')
      .setType(cc.FieldType.TEXT);
      
  fields.newDimension()
    .setId('group_name')
    .setName('Group name')
    .setDescription('The name of the group that the item sits in')
    .setType(cc.FieldType.TEXT);

  fields.newDimension()
    .setId('parent_id')
    .setName('Parent ID')
    .setDescription('The ID of the parent item (Sub-items only)')
    .setType(cc.FieldType.NUMBER);

  // Set the monday column_value columns
  let columns = col_res['data']['boards'][0]['columns']

  for (let column of columns) {
    switch (column["type"]) {
      case "checkbox":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            .setType(cc.FieldType.BOOLEAN);
        break;
      case "country":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.COUNTRY);
        break;
      case "date":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.YEAR_MONTH_DAY);
        break;
      case "file":
      case "link":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.URL);

        break;
      case "auto_number":
      case "numbers":
      case "item_id":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.NUMBER);
        break;
      case "hour":
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.HOUR);
        break;
      default:
        fields.newDimension()
            .setId(column['id'])
            .setName(column['title'])
            // .setDescription(column['description'])
            .setType(cc.FieldType.TEXT);
    }
  }
  return fields
}

function getSchema(request) {
  Logger.log(request);
  return cc.newGetSchemaResponse()
  .setFields(getFields(request))
  .build();
}

function getData(request) {
  Logger.log('Request: '+request)
  var mondayAPIKey = PropertiesService.getUserProperties().getProperty('dscc.key');

  var optionHeaders = {
    "Authorization" : mondayAPIKey,
    "API-Version": apiVersion
  }
  const board_id = request.configParams['board_id'];

  let query_1 = '{boards(ids: '+board_id+') { name items_page(limit: 100, query_params: {}) { cursor items { id name group { title } column_values { id text type value ... on MirrorValue { display_value } } } } } }'
  let options = {
    "method" : "post",
    "contentType" : "application/json",
    "headers" : optionHeaders,
    "payload" : JSON.stringify({
      "query" : query_1
    })
  };  
  
  let res = UrlFetchApp.fetch(url,options);
  let initial_request_json = JSON.parse(res);
  var jsonData = {};
  Logger.log("Initial request received: "+ JSON.stringify(initial_request_json))
  for (let item of initial_request_json['data']['boards'][0]['items_page']['items']) {
        let item_data = {}
        item_data['item_name'] = item.name;
        item_data['group_name'] = item.group.title;
        for (column of item['column_values']) {
          if (column.hasOwnProperty("display_value")) {
            item_data[column.id] = column.display_value;
          } else if (column.type == "board_relation") {
            item_data[column.id] = column.value;
          } else if (column.id != "subitems") {
            item_data[column.id] = column.text;
          } else {
        }
      }
      jsonData[item.id] = item_data;
  }
  var cursor_token = initial_request_json['data']['boards'][0]['items_page']['cursor']
  var caution_counter = 0
  while (cursor_token !== null) {
      caution_counter += 1
      let query_2 = '{ next_items_page( limit: 100 cursor: "' + cursor_token + '") { cursor items { id name group { title } column_values { id text type value... on MirrorValue {  display_value } } } } }'
      
      let options = {
      "method" : "post",
        "contentType" : "application/json",
        "headers" : optionHeaders,
        "payload" : JSON.stringify({
          "query" : query_2
        })
      };  
      let res = UrlFetchApp.fetch(url,options);
      let next_request_json = JSON.parse(res);
      cursor_token = next_request_json['data']['next_items_page']['cursor']
      let num_records = Object.keys(next_request_json.data.next_items_page.items.length)
      let next_items = next_request_json['data']['next_items_page']['items']
      for (let item of next_items) {
        let item_data = {}
        item_data['item_name'] = item.name;
        item_data['group_name'] = item.group.title;
        for (column of item['column_values']) {
          if (column.hasOwnProperty("display_value")) {
            item_data[column.id] = column.display_value;
          } else if (column.type == "board_relation") {
            item_data[column.id] = column.value;
          } else if (column.id != "subitems") {
            item_data[column.id] = column.text;
          } else {
        }
      }
      jsonData[item.id] = item_data;
    }
      if (caution_counter >= 1000) {
          Logger.log("Caution counter on request query reached "+ String(caution_counter) + " runs")
          break
      }
  }

  let requestedIds = request.fields.map(object => object['name']);
  let data = [];

  for (let item in jsonData) {
    Logger.log(JSON.stringify(jsonData[item]))
    let row = []
    for (let requestedId of requestedIds) {
      row.push(jsonData[item][requestedId])
    }
    data.push(row)
  }

  let fields = getFields(request).forIds(requestedIds);

  return cc.newGetDataResponse()
    .setFields(fields)
    .addAllRows(data)
    .build();
}

function isAdminUser() {
  return true;
}

