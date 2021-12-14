import json

with open('file.json') as f:
    data = json.load(f)

numCol = data['columns']
lenCol = len(numCol)
x = 0
pk_string = []

def tableName():
    for key, value in data.items():
        if key == 'dataStoreId':
            return(value[value.rindex('.')+1:])
    
print("CREATE TABLE", tableName(), "(")

for i in data['columns']:
    #print(i, "\n")
    a = i.keys()
    for j in a:
        #print(j, i[j])
        if i['dataType'] == 'NUMERIC':
            i['dataType'] = 'NUMBER'
            
        if i['isPrimaryKey'] == True:
            pk_string.append(i['name'])
            #print(pk_string)
            
        if i['isPrimaryKey'] == True:
            i['isPrimaryKey'] = ' PRIMARY KEY'
        else: 
            i['isPrimaryKey'] = ""
        
        
        if i['isPopulate'] == True:
            i['isPopulate'] = 'NOT NULL'
        else: i['isPopulate'] = ""

        if j == 'dataType' and i['dataType'] == 'TIMESTAMP':
            print(i['name']," ", i['dataType'], " ", i['isPopulate'], sep='', end='')
            #print('FIRST IF')
            if x < lenCol:
                print(',')
                x+=1
            
        if j == 'dataType' and i['dataType'] == 'DATE':
            print(i['name']," ", i['dataType'], " ", i['isPopulate'], sep='', end='')
            #print('SECOND IF')
            if x < lenCol:
                print(',')
                x+=1
            
        if j == 'name' and i['dataType'] != 'TIMESTAMP' and i['dataType'] != 'DATE':
            print(i['name']," ", i['dataType'], "(", i['size'], ")", " ",
                  i['isPopulate'], sep='', end='')
            if x < lenCol:
                #print(x)
                print(',')
                x+=1
pk_string = str(pk_string)[2:-2]
pk_string = pk_string.replace("'", "")
pk_string = pk_string.replace(" ", "")
print('CONSTRAINT my_pk PRIMARY KEY ({})'.format(pk_string))
#print( "CONSTRAINT my_pk PRIMARY KEY", "(", pk_string, ")")       
print(");")

 