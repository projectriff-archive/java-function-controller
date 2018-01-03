FROM scratch

ADD function-controller-linux /function-controller

ENTRYPOINT ["/function-controller"]
