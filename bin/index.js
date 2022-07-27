#!/usr/bin/env node
require('dotenv').config()
const yargs = require("yargs"); 
const _ = require("highland");
const fs = require("fs");
const csv = require("fast-csv");
const moment = require("moment");
const axios = require("axios").create();
var data = [];

const options = yargs
  .usage("Usage: -t <token>")
  .usage("Usage: -d <date>")
  .option("t", {
    alias: "token",
    describe: "e.g ETH,BTC,XRP,...",
    type: "string",
    demandOption: false,
  })
  .option("d", {
    alias: "date",
    describe: "Format: yyyy-MM-dd",
    type: "string",
    demandOption: false,
  }).argv;

const convert = async (tkn) => {
  try {
    const response = await axios.get(
      `https://min-api.cryptocompare.com/data/pricemulti?fsyms=${tkn}&tsyms=USD&api_key=${process.env.API_KEY}`
    );
    const js = await response.data;
    if (response.data.Response) {
      console.log("Could not fetch conversion API: ", response.data.Message);
      return -1;
    }

    return js;
  } catch (err) {
    console.log(err);
  }
};

const evaluate = async (token = -1, date = -1) => {
  let currentIndex = 0;
  const stream = _(
    fs.createReadStream("./transactions.csv").pipe(csv.parse({ headers: true }))
  )
    .map((data) => ({ ...data }))
    .batch(1000000)
    .each((entries) => {
      stream.pause();
      currentIndex += 1000000;
      //console.log("Created index :", currentIndex, entries);
      entries.forEach((s) => {
        data.push(s);
      });
      stream.resume();
    })
    .on("end", () => {
      console.log("data parsing completed, processing...");

      //if date is provided, filter by date
      if (date !== -1) {
        data = data.filter((item) => {
          return moment.unix(Number(item.timestamp)).isSame(date, "day");
        });
      }

      //group data by token
      grouped = data.reduce(function (r, a) {
        r[a.token] = r[a.token] || [];
        r[a.token].push(a);
        return r;
      }, Object.create(null));

      //if token is provided, filter the data by token
      if (token !== -1) {
        grouped = Object.keys(grouped)
          .filter((key) => key.includes(token))
          .reduce((cur, key) => {
            return Object.assign(cur, { [key]: grouped[key] });
          }, {});
      }

      if (Object.keys(grouped).length === 0) {
        console.log("No data matches the provided parameter(s)");
      } else {
        for (const tkn in grouped) {
          var dt = grouped[tkn];

          var deposits = dt.reduce(function (acc, obj) {
            return +(
              acc + Number(obj.transaction_type === "DEPOSIT" ? obj.amount : 0)
            ).toFixed(12);
          }, 0);

          var withdrawals = dt.reduce(function (acc, obj) {
            return +(
              acc +
              Number(obj.transaction_type === "WITHDRAWAL" ? obj.amount : 0)
            ).toFixed(12);
          }, 0);

          convert(tkn).then((js) => {
            if (js !== -1) {
              console.log(`____${tkn}____`);
              const result =
                Number(js[tkn].USD) * +(deposits - withdrawals).toFixed(4);

              console.log(`Value in USD: ${result}`);
            }
          });
        }
      }
      //process.exit();
    });
};
if (options.token && options.date) {
  evaluate(options.token, options.date);
} else if (options.token) {
  evaluate(options.token);
} else if (options.date) {
  evaluate(-1, options.date);
} else {
  evaluate();
}
