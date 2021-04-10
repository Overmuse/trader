# trader
`trader` is a rust application for efficiently sending trades to a brokerage. Currently, the `trader` application listens to `order-intent`s from kafka, and asynchronously sends those trades to `alpaca`.
