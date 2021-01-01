const { start, dispatch, spawnStateless, spawn } = require("nact");
const system = start();
const inFile = "./1.json";
const outFile = "./rez.json";
const jsonSize = 50;
const workers = 4;

const D_ActorInfo = {
  WORKER_SEND: "MESSAGE_FROM_DISTRIBUTOR_TO_WORKER",
  RESULT_ADD: "MESSAGE_FROM_DISTRIBUTOR_TO_RESULTWORKER_ADD",
  RESULT_COMPLETE: "MESSAGE_FROM_DISTRIBUTOR_TO_RESULT_COMPLETE",
  PRINTER: "MESSAGE_FROM_DISTRIBUTOR_TO_PRINTER",
  NAME: "DISTRIBUTOR_ACTOR",
};
const W_ActorInfo = {
  DISTRIBUTOR_SUCCESS: "MESSAGE_FROM_WORKER_TO_DISTRIBUTOR_SUCCESS",
  DISTRIBUTOR_FAIL: "MESSAGE_FROM_WORKER_TO_DISTRIBUTOR_FAIL",
};
const S_INFO = {
  SENDER_SEND: "MESSAGE_FROM_SENDER_TO_DISTRIBUTOR",
};
const R_ActorInfo = {
  DISTRIBUTOR_COMPLETE: "MESSAGE_FROM_RESULT_TO_DISTRIBUTOR_COMPLETE",
  NAME: "RESULT",
};
const P_ActorInfo = {
  NAME: "PRINTER",
};

/*
distributorActor
Children:workerActor[...],resultActor,printActor
Send messages between actors
*/
const distributorActor = spawn(
  system,
  (state = { completed: 0, currentWorkerRank: 0 }, msg, ctx) => {
    switch (msg.type) {
      case S_INFO.SENDER_SEND: {
        dispatch(ctx.children.get(state.currentWorkerRank.toString()), { type: D_ActorInfo.WORKER_SEND, student: msg.student });
        const next = state.currentWorkerRank + 1;
        if (next === workers) {
          return { ...state, currentWorkerRank: 0 };
        } else {
          return { ...state, currentWorkerRank: next };
        }
      }
      case W_ActorInfo.DISTRIBUTOR_SUCCESS: {
        dispatch(ctx.children.get(R_ActorInfo.NAME), { type: D_ActorInfo.RESULT_ADD, student: msg.student });
        const completedCount = state.completed + 1;
        if (completedCount == jsonSize) dispatch(ctx.children.get(R_ActorInfo.NAME), { type: D_ActorInfo.RESULT_COMPLETE, student: "" });
        return { ...state, completed: completedCount };
      }
      case W_ActorInfo.DISTRIBUTOR_FAIL: {
        const completedCount = state.completed + 1;
        if (completedCount == jsonSize) dispatch(ctx.children.get(R_ActorInfo.NAME), { type: D_ActorInfo.RESULT_COMPLETE, student: "" });
        return { ...state, completed: completedCount };
      }
      case R_ActorInfo.DISTRIBUTOR_COMPLETE: {
        dispatch(ctx.children.get(P_ActorInfo.NAME), { type: D_ActorInfo.PRINTER, students: msg.students });
        break;
      }
    }
  },
  D_ActorInfo.NAME
);

/*
workerActor
Parent:distributorActor
Receive student information from distributor, calculate hash and return it
*/
const workerActor = (parent, rank) =>
  spawnStateless(
    parent,
    (msg) => {
      const hash = hashSum(msg.student.name);
      if (hash > 20) {
        msg.student.hash = hash;
        dispatch(parent, { type: W_ActorInfo.DISTRIBUTOR_SUCCESS, student: msg.student });
      } else {
        dispatch(parent, { type: W_ActorInfo.DISTRIBUTOR_FAIL, student: msg.student });
      }
    },
    rank
  );

/*
resultActor
Parent:distributorActor
Collect results
 */
const resultActor = (parent) =>
  spawn(
    parent,
    (state = { students: [] }, msg) => {
      switch (msg.type) {
        case D_ActorInfo.RESULT_ADD: {
          state.students.push(msg.student);
          state.students.sort(function (a, b) {
            return a.hash - b.hash;
          });
          return { students: state.students };
        }
        case D_ActorInfo.RESULT_COMPLETE: {
          dispatch(parent, { type: R_ActorInfo.DISTRIBUTOR_COMPLETE, students: state.students });
          break;
        }
      }
    },
    R_ActorInfo.NAME
  );

/*
printActor
Parent:distributorActor
Print results into file
*/
const printActor = (parent) =>
  spawnStateless(
    parent,
    (msg) => {
      const top =
        "|--------------------------------------------|" +
        "\n" +
        "|" +
        "name".padEnd(15) +
        "|" +
        "year".toString().padEnd(8) +
        "|" +
        "grade".toString().padEnd(8) +
        "|" +
        "hash".toString().padEnd(10) +
        "|" +
        "\n" +
        "|--------------------------------------------|" +
        "\n";
      const bottom = "|--------------------------------------------|" + "\n";
      const wr = require("fs").createWriteStream(outFile);

      wr.write("PRADINIS FAILAS".padStart(28) + "\n");
      wr.write(top);
      require(inFile).forEach((item) => {
        wr.write("|" + item.name.padEnd(15) + "|" + item.year.toString().padEnd(8) + "|" + item.grade.toString().padEnd(8) + "|" + "0".toString().padEnd(10) + "|" + "\n");
      });
      wr.write(bottom);

      wr.write("REZULTATAI".padStart(28) + "\n");
      wr.write(top);
      msg.students.forEach((item) => {
        wr.write("|" + item.name.padEnd(15) + "|" + item.year.toString().padEnd(8) + "|" + item.grade.toString().padEnd(8) + "|" + item.hash.toString().padEnd(10) + "|" + "\n");
      });
      wr.write(bottom);

      wr.end();
    },
    P_ActorInfo.NAME
  );

/*
sender
Send information from file to distributor
*/
function sender() {
  require(inFile).forEach((item) => {
    dispatch(distributorActor, { type: S_INFO.SENDER_SEND, student: item });
  });
}

function hashSum(name) {
  const hashCode = (string) => string.split("").reduce((a, b) => ((a << 2) - a + b.charCodeAt(0)) | 0, 0);
  return hashCode(name)
    .toString()
    .split("")
    .reduce((acc, cur) => acc + +cur, 0);
}

function main() {
  console.clear();
  resultActor(distributorActor);
  printActor(distributorActor);
  [...Array(workers).keys()].forEach((rank) => workerActor(distributorActor, rank.toString()));
  sender();
}

main();
