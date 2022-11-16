for i in {1..10}; do go test -race -run 2A; done
for i in {1..10}; do go test -race -run 2B; done
for i in {1..10}; do go test -race -run 2C; done
for i in {1..10}; do go test -race -run 2D; done