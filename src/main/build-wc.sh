echo "Building wc plugin..."
cd src/main
go build -buildmode=plugin -gcflags="all=-N -l" ../mrapps/wc.go
echo "wc plugin built"
rm -f mr-*
echo "deleted old mr-* files"
