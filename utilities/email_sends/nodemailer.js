import nodemailer from "nodemailer";

import path from "path";
import { fileURLToPath } from "url";

import dotenv from "dotenv";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

// First, define send settings by creating a new transporter:
const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com", // SMTP server address (usually mail.your-domain.com)
    pool: true,
    port: 587, // Port for SMTP (usually 465)
    secure: false, // Usually true if connecting to port 465
    auth: {
        user: process.env.EMAIL_SENDER,
        pass: process.env.EMAIL_PASSWORD, // SET IN GMAIL UNDER 2-FACTOR AUTH
    },
});

const verify_transporter_connection = async () => {
    // verify connection configuration
    transporter.verify(function (error, success) {
        if (error) {
            console.log(error);
        } else {
            console.log("External Email Server is ready to take our messages");
            // console.log(success);
        }
    });
};

function close_mail_transport() {
    try {
        transporter.close();
    } catch { }
};

const mail_details = (args) => {
    // construct mail details/options object
    const mail_options = {
        from: {
            name: "The Attendance Tracker",
            address: "callasteven@gmail.com",
        },
        to: "callasteven@gmail.com",
        subject: "test",
        text: "test content",
        html: "test content2",
    };

    // pass mail options to send_mail
    // send_mail(args, mail_options);

    return mail_options;
};

const send_mail = async (mail_options) => {
    let info;
    verify_transporter_connection();

    try {
        info = await transporter.sendMail(mail_options);
        console.log("Email sent: " + info.response);
        return info;
    } catch (error) {
        console.log(error);
    } finally {
        // process.exit(0); // ensure the process exits to the command line
    }
};

// await send_mail(mail_details());



export {
    mail_details,
    send_mail,
    close_mail_transport,
};

// SECTION - SOURCES
// https://www.youtube.com/watch?v=QDIOBsMBEI0
// https://openjavascript.info/2023/01/10/nodemailer-tutorial-send-emails-in-node-js/#Basic%20example
// https://mailtrap.io/blog/sending-emails-with-nodemailer/
// templates https://codedmails.com/reset-emails-preview
// mock app at /Users/stevecalla/du_coding/utilities/node-mailer/index.js