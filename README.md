# Simple Garbage Collectors

![Unit Tests](https://github.com/dbgroup-nagoya-u/bztree/workflows/Unit%20Tests/badge.svg?branch=main)

## 既知の不具合

メモリが解放されたことを確認するために、単体テストの内部で**解放済みの領域にアクセス**しています。このため、単体テストがたまに失敗していまいます。明らかに良くない方法なので、適切な方法がわかったら単体テストの実装をそちらに変えます。
