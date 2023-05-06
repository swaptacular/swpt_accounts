;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")

((python-mode
  . ((eglot-workspace-configuration
      . (:pylsp (:plugins (:pycodestyle (:enabled :json-false)
                           :mccabe (:enabled :json-false)
                           :pyflakes (:enabled :json-false)
                           :flake8 (:enabled t))
                 :configurationSources ["flake8"]))))))
