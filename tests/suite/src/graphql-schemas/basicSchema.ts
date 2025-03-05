export const BasicSchema = `
type BasicSchema @createModel(accountRelation: LIST, description: "A set of unique numbers")
@createIndex(fields: [{ path: "myData" }]){
  myData: String! @string(maxLength: 500)
}
`
