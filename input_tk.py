import tkinter as tk
import tkinter.messagebox as tmsg

def ButtonClick():
    tmsg.showinfo(editBox1.get())

root = tk.Tk()
root.geometry('400x150')
root.title('Instagram DB Input')

label = tk.label(root,text='ユーザID',font = ('Helvetica',14))
label.place(x=20,y=20)

editBox1 = tk.Entry(width=4,font =('Helvetiva',26))
editBox1.place(x=120,y=60)

button1 = tk.Button(root,text='Submit',font = ('Helvetica',14),command=ButtonClick)
button1.place(x=220,y=60)

root.mainloop()
