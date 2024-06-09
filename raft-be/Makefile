generate:
	protoc --proto_path=proto \
			--go_out=. \
			--go-grpc_out=. \
			proto/*.proto

enable-np:
	tc qdisc add dev lo root netem delay 200ms 50ms reorder 8% corrupt 5% duplicate 2% 5% loss 5%

disable-np:
	tc qdisc del dev lo root netem delay 200ms 50ms reorder 8% corrupt 5% duplicate 2% 5% loss 5%
