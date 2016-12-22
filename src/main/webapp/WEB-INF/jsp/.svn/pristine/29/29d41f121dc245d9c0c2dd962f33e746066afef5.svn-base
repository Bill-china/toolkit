<?php

class ComEmail
{
    function send_out_mail($title, $body, $email_list)
    {
        $data = array();

        $mail = array(
            'batch' => true,                           // required, bool
            'subject' => $title, // required, string
            'from' => 'no-reply@ucmail.360.cn',              // required, string, 如果是向外域发邮件，此参数必须为：'no-reply@ucmail.360.cn'
            'to' => $email_list,                // optional, string or array
            'cc' => array(),
            'body' => $body,                 // required, string
            'format' => 'html'                         // optional, 'html' or 'plain'
        );

        $data['mail'] = json_encode($mail);

        $data['token'] = 'apKlYZAmhQB5AcjE';

        $url = 'http://qms.addops.soft.360.cn:8360/interface/ext_deliver.php';

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        $response = curl_exec($ch);
        if (curl_errno($ch)) 
        {
            return curl_error($ch);
        }
        curl_close($ch);

        return $response;
    }

    function send_inner_mail($title, $body, $email_list, $cc_email_list = array())
    {
        $data = array();

        $mail = array(
            'batch' => true,                           // required, bool
            'subject' => $title, // required, string
            'from' => 'fenghailong@alarm.360.cn',              // required, string, 如果是向外域发邮件，此参数必须为：'no-reply@ucmail.360.cn'
            'to' => $email_list,                // optional, string or array
            'cc' => $cc_email_list,
            'body' => $body,                 // required, string
            'format' => 'html'                         // optional, 'html' or 'plain'
        );

        $data['mail'] = json_encode($mail);

        $url = 'http://qms.addops.soft.360.cn:8360/interface/deliver.php';

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        $response = curl_exec($ch);
        if (curl_errno($ch)) 
        {
            return curl_error($ch);
        }
        curl_close($ch);

        return $response;
    }
    
    function send_attachment_mail($title, $body, $email_list, $cc_email_list = array(), $attachment = '')
    {
        $data = array();

        $mail = array(
            'batch' => true,                           // required, bool
            'subject' => $title, // required, string
            //'from' => 'no-reply@ucmail.360.cn',             // required, string, 如果是向外域发邮件，此参数必须为：'no-reply@ucmail.360.cn'
            'from' => 'no-reply@ucmail.qihoo.com',
            'to' => $email_list,                // optional, string or array
            'cc' => $cc_email_list,
            'body' => $body,                 // required, string
            'format' => 'html'               // optional, 'html' or 'plain'
        );

        $data['mail'] = json_encode($mail);
        $data['token'] = 'apKlYZAmhQB5AcjE';
        if($attachment) $data['file1'] = "@" . $attachment;
        $url = 'http://qms.addops.soft.360.cn:8360/interface/ext_deliver.php';
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        $response = curl_exec($ch);
        if (curl_errno($ch)) 
        {
            return curl_error($ch);
        }
        curl_close($ch);

        return $response;
    }
}

?>