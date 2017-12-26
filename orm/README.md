# ORM
===
简化sql操作，提供基于schema，model，query组合

## 初始化
+ 使用sql.DB，内部不处理数据链接
```
executor := NewExecutor()
executor.Conn(conn)
```
+ 使用事务
```
executor.Session(func(conn Conn) error {
    executor.Query().Session(conn).Select()
})
```
or
```
tx, err := executor.Begin()
executor.Query().Session(tx).Select()
tx.Commit()
tx.Rollback()
```

## 基础查询
+ 基于Query，提供链式查询
```
query := executor.Query().Table().Field().Where().SelectString()
query := executor.Query().Sql("select * from table where name=?", []interface{}{"t"})
```
+ 便捷操作
```
query.Order().Asc().Desc().Limit(0,1).Page(1,20)
```
+ 增删改
```
query.Insert(data) // data可以是map[string]类型或者struct
query.Update(data) // data可以是map[string]类型或者struct
query.Delete()
```
+ 绑定结构体
```
query.Find(struct)  // struct为单条查询
query.Find(slice) // slice为多条查询
```
+ 参数绑定
```
type ttt struct{
    Name string
}
t := &ttt{
    Name: "t"
}
t := map[string]interface{}{Name:"t"}
query.Sql("select * from table where a = :Name", t).SelectString()
```

## 表结构
+ 根据struct确定表结构
```
model, err := NewModel("user", &Users{}, nil)
```
或者
```
schema, err := NewSchema(&Users{}, nil)
model := NewModelWithSchema("user", schema, nil)
```
+ model绑定数据链接，schema绑定表结构
+ 自定义表结构
```
schema.SetAutoIncrement("id")
schema.SoftDelete() // 开启软删除
schema.SetTimeStamps() // 开启时间更新
schema.SetPrimary() // 设置主键
```
+ 绑定事件
```
schema.On("beforeInsert", func)
schema.On("afterInsert", func)
```

## 模型查询
+ 绑定主键
```
model.PK() // 需要和schema中的Primary长度一致
```
+ 链式查询，支持Query的接口
```
model.PK().Get() // 查询主键记录
model.Query().Session(conn) // 绑定事务
model.Query().Save(data) // 实例
model.PK().SoftDelete() // 软删除
model.PK().Recovery() // 恢复删除
```
+ 绑定查询
```
query := executor.Query().Session(conn)
// 判断软删除
model.Find(query)
model.Count(query)
model.Update(query, data)
model.Delete(query)

model.FindByQuery(query)
model.CountQuery(query)
model.UpdateQuery(query, data)
model.DeleteQuery(query)
```
